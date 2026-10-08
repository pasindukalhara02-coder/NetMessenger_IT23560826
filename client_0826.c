#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#define SERVER_IP "127.0.0.1"
#define PORT 6826
#define NID 5608

#define LINE_SIZE 2048
#define IO_BUFFER_SIZE 8192
#define MAX_FILE_SIZE (5ULL * 1024ULL * 1024ULL)
#define USERNAME_LEN 50
#define FILENAME_LEN 256
#define TARGET_LEN 50

static int sock_fd = -1;
static volatile int running = 1;
static pthread_t receiver_thread;

/* ------------------------------------------------------------------
   Buffered receiver.

   A single recv() may contain:
   - a partial line,
   - several lines, or
   - a FILE header followed immediately by raw bytes.
   The buffer keeps unread bytes so framing is preserved.
   ------------------------------------------------------------------ */
typedef struct
{
    int fd;
    unsigned char buffer[IO_BUFFER_SIZE];
    size_t start;
    size_t end;
} conn_reader_t;

static int reader_refill(conn_reader_t *reader)
{
    if (reader->start < reader->end)
        return 1;

    reader->start = 0;
    reader->end = 0;

    for (;;)
    {
        ssize_t n = recv(reader->fd,
                         reader->buffer,
                         sizeof(reader->buffer),
                         0);

        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (n == 0)
            return 0;

        reader->end = (size_t)n;
        return 1;
    }
}

static ssize_t reader_line(conn_reader_t *reader,
                           char *output,
                           size_t output_size)
{
    size_t used = 0;

    if (output == NULL || output_size == 0)
        return -2;

    while (1)
    {
        size_t i;

        if (reader->start == reader->end)
        {
            int rc = reader_refill(reader);

            if (rc == 0)
            {
                if (used == 0)
                    return 0;

                output[used] = '\0';
                return (ssize_t)used;
            }

            if (rc < 0)
                return -1;
        }

        for (i = reader->start; i < reader->end; ++i)
        {
            if (reader->buffer[i] == '\n')
            {
                size_t count = i - reader->start;

                if (used + count >= output_size)
                {
                    reader->start = i + 1;
                    return -2;
                }

                memcpy(output + used,
                       reader->buffer + reader->start,
                       count);

                used += count;
                reader->start = i + 1;

                while (used > 0 && output[used - 1] == '\r')
                    --used;

                output[used] = '\0';
                return (ssize_t)used;
            }
        }

        if (used + (reader->end - reader->start) >= output_size)
        {
            reader->start = reader->end;
            return -2;
        }

        memcpy(output + used,
               reader->buffer + reader->start,
               reader->end - reader->start);

        used += reader->end - reader->start;
        reader->start = reader->end;
    }
}

/* Read exactly length raw bytes from the same buffered TCP stream. */
static int reader_exact(conn_reader_t *reader,
                        void *data,
                        uint64_t length)
{
    unsigned char *destination = (unsigned char *)data;

    while (length > 0)
    {
        if (reader->start < reader->end)
        {
            size_t available = reader->end - reader->start;
            size_t take = available;

            if ((uint64_t)take > length)
                take = (size_t)length;

            memcpy(destination,
                   reader->buffer + reader->start,
                   take);

            reader->start += take;
            destination += take;
            length -= (uint64_t)take;
            continue;
        }

        {
            size_t want = (length > IO_BUFFER_SIZE)
                        ? IO_BUFFER_SIZE
                        : (size_t)length;

            ssize_t n = recv(reader->fd,
                             destination,
                             want,
                             0);

            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                return -1;
            }

            if (n == 0)
                return -1;

            destination += (size_t)n;
            length -= (uint64_t)n;
        }
    }

    return 0;
}

/* ------------------------------------------------------------------ */
static int send_all(int fd, const void *data, size_t length)
{
    const unsigned char *pointer = (const unsigned char *)data;

    while (length > 0)
    {
        ssize_t sent = send(fd,
                            pointer,
                            length,
                            MSG_NOSIGNAL);

        if (sent < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (sent == 0)
            return -1;

        pointer += (size_t)sent;
        length -= (size_t)sent;
    }

    return 0;
}

static int send_command(const char *command)
{
    char line[LINE_SIZE + 2];

    if (command == NULL)
        return -1;

    if (snprintf(line, sizeof(line), "%s\n", command) >= (int)sizeof(line))
    {
        fprintf(stderr, "Command is too long.\n");
        return -1;
    }

    return send_all(sock_fd, line, strlen(line));
}

/* ------------------------------------------------------------------ */
static int mkdir_if_needed(const char *path)
{
    struct stat st;

    if (mkdir(path, 0755) == 0)
        return 0;

    if (errno == EEXIST &&
        stat(path, &st) == 0 &&
        S_ISDIR(st.st_mode))
        return 0;

    return -1;
}

static int safe_basename(const char *input,
                         char *output,
                         size_t output_size)
{
    const char *base;
    const char *slash;
    const char *backslash;
    size_t length;

    if (input == NULL ||
        output == NULL ||
        output_size < 2 ||
        input[0] == '\0')
        return -1;

    if (strcmp(input, ".") == 0 || strcmp(input, "..") == 0)
        return -1;

    if (strstr(input, "..") != NULL)
        return -1;

    slash = strrchr(input, '/');
    backslash = strrchr(input, '\\');

    base = input;

    if (slash != NULL && slash + 1 > base)
        base = slash + 1;

    if (backslash != NULL && backslash + 1 > base)
        base = backslash + 1;

    if (*base == '\0')
        return -1;

    length = strlen(base);
    if (length >= output_size)
        return -1;

    if (base[0] == '.')
        return -1;

    memcpy(output, base, length);
    output[length] = '\0';
    return 0;
}

/* ------------------------------------------------------------------
   Receive a FILE header followed by exactly <filesize> raw bytes.
   Files are stored locally under:
       received_files/<sender>/<filename>
   ------------------------------------------------------------------ */
static int receive_file(conn_reader_t *reader,
                        const char *sender,
                        const char *filename,
                        uint64_t file_size)
{
    char safe_sender[USERNAME_LEN];
    char safe_filename[FILENAME_LEN];
    char directory[512];
    char path[1024];
    FILE *file;
    unsigned char buffer[IO_BUFFER_SIZE];
    uint64_t remaining = file_size;

    if (file_size > MAX_FILE_SIZE)
        return -1;

    if (safe_basename(sender,
                      safe_sender,
                      sizeof(safe_sender)) < 0)
        return -1;

    if (safe_basename(filename,
                      safe_filename,
                      sizeof(safe_filename)) < 0)
        return -1;

    if (mkdir_if_needed("received_files") < 0)
        return -1;

    snprintf(directory,
             sizeof(directory),
             "received_files/%s",
             safe_sender);

    if (mkdir_if_needed(directory) < 0)
        return -1;

    snprintf(path,
             sizeof(path),
             "%s/%s",
             directory,
             safe_filename);

    file = fopen(path, "wb");
    if (file == NULL)
        return -1;

    while (remaining > 0)
    {
        size_t chunk = remaining > sizeof(buffer)
                     ? sizeof(buffer)
                     : (size_t)remaining;

        if (reader_exact(reader, buffer, (uint64_t)chunk) < 0)
        {
            fclose(file);
            unlink(path);
            return -1;
        }

        if (fwrite(buffer, 1, chunk, file) != chunk)
        {
            fclose(file);
            unlink(path);
            return -1;
        }

        remaining -= (uint64_t)chunk;
    }

    if (fclose(file) != 0)
    {
        unlink(path);
        return -1;
    }

    printf("\n[FILE] Received from %s\n", safe_sender);
    printf("       Saved to : %s\n", path);
    printf("       File size: %" PRIu64 " bytes\n", file_size);
    printf("> ");
    fflush(stdout);

    return 0;
}

/* ------------------------------------------------------------------ */
static int receive_server_line(int fd, char *line, size_t line_size)
{
    size_t used = 0;

    while (used + 1 < line_size)
    {
        char ch;
        ssize_t n = recv(fd, &ch, 1, 0);

        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (n == 0)
        {
            if (used == 0)
                return 0;
            break;
        }

        if (ch == '\n')
            break;

        if (ch != '\r')
            line[used++] = ch;
    }

    line[used] = '\0';
    return (int)used;
}

/* ------------------------------------------------------------------ */
static void print_help(void)
{
    printf("\n============================================================\n");
    printf("                 NetMessenger Commands\n");
    printf("============================================================\n");
    printf(" BASIC\n");
    printf("   LIST                         Show online users\n");
    printf("   BCAST <message>             Send to all other users\n");
    printf("   PMSG <user> <message>       Send a private message\n");
    printf("\n ROOMS\n");
    printf("   JOIN <room>                 Join/create a room\n");
    printf("   LEAVE <room>                Leave a room\n");
    printf("   ROOMS                       Show available rooms\n");
    printf("   RMSG <room> <message>       Send a room message\n");
    printf("\n FILE SHARING\n");
    printf("   SENDFILE <target> <file> <size>\n");
    printf("                              Send a file to a user/room\n");
    printf("   File limit: 5 MB\n");
    printf("\n SESSION\n");
    printf("   HELP                        Show this help\n");
    printf("   QUIT                        Disconnect cleanly\n");
    printf("\n EXAMPLE\n");
    printf("   BCAST Hello everyone!\n");
    printf("   PMSG kalhara2 Hello\n");
    printf("   JOIN room1\n");
    printf("   RMSG room1 Hello room!\n");
    printf("   SENDFILE kalhara2 testfile.txt 44\n");
    printf("============================================================\n\n");
}

/* ------------------------------------------------------------------
   Receiver thread starts only after registration succeeds.
   ------------------------------------------------------------------ */
static void *receive_messages(void *arg)
{
    int fd = *(int *)arg;
    conn_reader_t reader;
    char line[LINE_SIZE];

    reader.fd = fd;
    reader.start = 0;
    reader.end = 0;

    while (running)
    {
        ssize_t n = reader_line(&reader,
                                line,
                                sizeof(line));

        if (n == 0)
        {
            if (running)
                printf("\n[SERVER] Connection closed by server.\n");

            running = 0;
            break;
        }

        if (n == -2)
        {
            printf("\n[ERROR] Server sent an overlong response line.\n> ");
            fflush(stdout);
            continue;
        }

        if (n < 0)
        {
            if (running && errno != ECONNRESET)
                fprintf(stderr, "[ERROR] Receive failed: %s\n", strerror(errno));

            running = 0;
            break;
        }

        if (strncmp(line, "FILE ", 5) == 0)
        {
            char sender[USERNAME_LEN];
            char filename[FILENAME_LEN];
            unsigned long long parsed_size = 0;

            if (sscanf(line,
                       "FILE %49s %255s %llu",
                       sender,
                       filename,
                       &parsed_size) != 3)
            {
                printf("\n[FILE] Invalid file header received from server.\n> ");
                fflush(stdout);
                running = 0;
                break;
            }

            if ((uint64_t)parsed_size > MAX_FILE_SIZE)
            {
                printf("\n[ERROR] Received file exceeds the 5 MB limit.\n> ");
                fflush(stdout);
                running = 0;
                break;
            }

            if (receive_file(&reader,
                             sender,
                             filename,
                             (uint64_t)parsed_size) < 0)
            {
                printf("\n[ERROR] File transfer failed.\n> ");
                fflush(stdout);
                running = 0;
                break;
            }

            continue;
        }

        if (strncmp(line, "ERR ", 4) == 0)
            printf("\n[ERROR] Server: %s\n", line);
        else if (strncmp(line, "OK ", 3) == 0)
            printf("\n[SUCCESS] Server: %s\n", line);
        else
            printf("\n[INFO] Server: %s\n", line);
        printf("> ");
        fflush(stdout);

        if (strcmp(line, "OK BYE NID:5608") == 0)
        {
            running = 0;
            break;
        }
    }

    return NULL;
}

/* ------------------------------------------------------------------
   Connect and perform the mandatory first REGISTER command.
   Registration is synchronous so duplicate usernames can be retried
   without starting the asynchronous receiver thread yet.
   ------------------------------------------------------------------ */
static int connect_and_register(void)
{
    struct sockaddr_in server_address;
    char welcome[LINE_SIZE];
    char response[LINE_SIZE];
    char username[USERNAME_LEN];

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0)
    {
        fprintf(stderr, "[ERROR] Could not create client socket: %s\n", strerror(errno));
        return -1;
    }

    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PORT);

    if (inet_pton(AF_INET,
                  SERVER_IP,
                  &server_address.sin_addr) <= 0)
    {
        perror("inet_pton");
        close(sock_fd);
        sock_fd = -1;
        return -1;
    }

    if (connect(sock_fd,
                (struct sockaddr *)&server_address,
                sizeof(server_address)) < 0)
    {
        fprintf(stderr, "[ERROR] Could not connect to %s:%d: %s\n",
                SERVER_IP, PORT, strerror(errno));
        close(sock_fd);
        sock_fd = -1;
        return -1;
    }

    printf("============================================================\n");
    printf("              NetMessenger Client\n");
    printf("============================================================\n");
    printf("Connected to NetMessenger server.\n");
    printf("Server: %s:%d\n", SERVER_IP, PORT);
    printf("NID: NID:%d\n", NID);
    printf("============================================================\n");

    if (receive_server_line(sock_fd,
                            welcome,
                            sizeof(welcome)) <= 0)
    {
        fprintf(stderr, "[ERROR] Failed to receive the server welcome message.\n");
        close(sock_fd);
        sock_fd = -1;
        return -1;
    }

    printf("Server: %s\n", welcome);

    while (1)
    {
        char command[LINE_SIZE];
        size_t length;
        const char *prefix = "OK REGISTERED ";

        printf("Enter username: ");
        fflush(stdout);

        if (fgets(username, sizeof(username), stdin) == NULL)
        {
            close(sock_fd);
            sock_fd = -1;
            return -1;
        }

        length = strcspn(username, "\r\n");
        username[length] = '\0';

        if (username[0] == '\0')
        {
            printf("[ERROR] Username cannot be empty. Please try again.\n");
            continue;
        }

        if (snprintf(command,
                     sizeof(command),
                     "REGISTER %s",
                     username) >= (int)sizeof(command))
        {
            printf("[ERROR] Username is too long. Please use a shorter name.\n");
            continue;
        }

        if (send_command(command) < 0)
        {
            fprintf(stderr, "[ERROR] Send failed: %s\n", strerror(errno));
            close(sock_fd);
            sock_fd = -1;
            return -1;
        }

        if (receive_server_line(sock_fd,
                                response,
                                sizeof(response)) <= 0)
        {
            fprintf(stderr, "Server closed the connection during registration.\n");
            close(sock_fd);
            sock_fd = -1;
            return -1;
        }

        if (strncmp(response, "ERR ", 4) == 0)
            printf("[ERROR] Server: %s\n", response);
        else
            printf("[SUCCESS] Server: %s\n", response);

        if (strncmp(response,
                    prefix,
                    strlen(prefix)) == 0)
        {
            return 0;
        }

        printf("[INFO] Registration was not accepted. Please choose another username.\n");
    }
}

/* ------------------------------------------------------------------ */
static int get_file_size(const char *filename, uint64_t *size_out)
{
    struct stat st;

    if (stat(filename, &st) < 0)
        return -1;

    if (!S_ISREG(st.st_mode))
    {
        errno = EINVAL;
        return -1;
    }

    if (st.st_size < 0)
    {
        errno = EOVERFLOW;
        return -1;
    }

    *size_out = (uint64_t)st.st_size;
    return 0;
}

/* ------------------------------------------------------------------ */
static int send_file_command(const char *input_command)
{
    char target[TARGET_LEN];
    char filename[FILENAME_LEN];
    unsigned long long user_size = 0;
    uint64_t actual_size;
    FILE *file;
    unsigned char buffer[IO_BUFFER_SIZE];
    char header[LINE_SIZE];
    uint64_t remaining;
    char extra[2];
    int fields;

    /*
     * Keep malformed SENDFILE commands on the wire so the server can
     * exercise and report the protocol-level error.
     */
    fields = sscanf(input_command,
                    "SENDFILE %49s %255s %llu %1s",
                    target,
                    filename,
                    &user_size,
                    extra);

    if (fields != 3)
    {
        if (send_command(input_command) < 0)
        {
            fprintf(stderr, "[ERROR] Could not send malformed SENDFILE command.\n");
            running = 0;
            return -1;
        }

        printf("[INFO] SENDFILE format sent to server for validation.\n");
        return 0;
    }

    if (get_file_size(filename, &actual_size) < 0)
    {
        printf("[ERROR] Cannot access local file '%s': %s\n",
               filename,
               strerror(errno));
        return 0;
    }

    if (actual_size != (uint64_t)user_size)
    {
        printf("[ERROR] Specified filesize (%llu) does not match actual file size (%" PRIu64 ").\n",
               user_size,
               actual_size);
        return 0;
    }

    if (actual_size > MAX_FILE_SIZE)
    {
        printf("[ERROR] File is larger than the 5 MB limit.\n");
        return 0;
    }

    file = fopen(filename, "rb");
    if (file == NULL)
    {
        printf("[ERROR] Cannot open '%s': %s\n", filename, strerror(errno));
        return 0;
    }

    if (snprintf(header,
                 sizeof(header),
                 "SENDFILE %s %s %llu\n",
                 target,
                 filename,
                 user_size) >= (int)sizeof(header))
    {
        printf("[ERROR] SENDFILE command is too long.\n");
        fclose(file);
        return 0;
    }

    printf("[FILE] Sending '%s' (%" PRIu64 " bytes) -> %s\n",
           filename,
           actual_size,
           target);

    /* Protocol: header newline, immediately followed by raw bytes. */
    if (send_all(sock_fd, header, strlen(header)) < 0)
    {
        fprintf(stderr, "[ERROR] Failed to send SENDFILE header: %s\n", strerror(errno));
        fclose(file);
        running = 0;
        return -1;
    }

    remaining = actual_size;

    while (remaining > 0)
    {
        size_t chunk = remaining > sizeof(buffer)
                     ? sizeof(buffer)
                     : (size_t)remaining;

        size_t got = fread(buffer, 1, chunk, file);

        if (got != chunk)
        {
            fprintf(stderr, "[ERROR] Failed while reading '%s'.\n", filename);
            fclose(file);
            running = 0;
            return -1;
        }

        if (send_all(sock_fd, buffer, got) < 0)
        {
            fprintf(stderr, "[ERROR] Failed while sending file data: %s\n", strerror(errno));
            fclose(file);
            running = 0;
            return -1;
        }

        remaining -= (uint64_t)got;
    }

    fclose(file);

    printf("[FILE] File data sent. Waiting for server response...\n");
    return 0;
}

/* ------------------------------------------------------------------ */
int main(void)
{
    char command[LINE_SIZE];

    if (connect_and_register() < 0)
        return EXIT_FAILURE;

    running = 1;

    printf("\nRegistration successful.\n");
    print_help();

    if (pthread_create(&receiver_thread,
                       NULL,
                       receive_messages,
                       &sock_fd) != 0)
    {
        perror("pthread_create");
        close(sock_fd);
        return EXIT_FAILURE;
    }

    while (running)
    {
        char *newline;

        printf("> ");
        fflush(stdout);

        if (fgets(command, sizeof(command), stdin) == NULL)
        {
            if (running)
            {
                printf("\n[INFO] Input closed. Requesting clean disconnect...\n");
                send_command("QUIT");
            }
            break;
        }

        newline = strpbrk(command, "\r\n");
        if (newline != NULL)
            *newline = '\0';

        if (command[0] == '\0')
            continue;

        if (strcmp(command, "HELP") == 0)
        {
            print_help();
            continue;
        }

        if (strcmp(command, "SENDFILE") == 0 ||
            strncmp(command, "SENDFILE ", 9) == 0)
        {
            int rc = send_file_command(command);

            if (rc < 0)
                break;

            continue;
        }

        if (send_command(command) < 0)
        {
            perror("send");
            running = 0;
            break;
        }

        if (strcmp(command, "QUIT") == 0)
            break;
    }

    running = 0;
    shutdown(sock_fd, SHUT_RDWR);
    pthread_join(receiver_thread, NULL);
    close(sock_fd);
    sock_fd = -1;

    printf("Client connection closed.\n");
    return EXIT_SUCCESS;
}
