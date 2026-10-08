#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>

#define PORT 6826
#define NID 5608
#define BACKLOG 20

#define BUFFER_SIZE 1024
#define LINE_SIZE 2048
#define IO_BUFFER_SIZE 8192
#define MAX_USERS 100
#define MAX_ROOMS 100
#define USERNAME_LEN 50
#define ROOM_NAME_LEN 50
#define MAX_MESSAGE_LEN 1800
#define MAX_FILENAME_LEN 256
#define MAX_TARGET_LEN 50
#define MAX_FILE_SIZE (5ULL * 1024ULL * 1024ULL)

#define LOG_FILE "netmsg_IT23560826.log"
#define STORAGE_ROOT "storage/IT23560826"

/* ------------------------------------------------------------------
   Per-connection receive buffer.
   It allows one recv() to contain partial/multiple text lines and
   also allows SENDFILE raw bytes to remain in the same TCP buffer.
   ------------------------------------------------------------------ */
typedef struct
{
    int fd;
    unsigned char buffer[IO_BUFFER_SIZE];
    size_t start;
    size_t end;
} conn_reader_t;

typedef struct
{
    int socket_fd;
    struct sockaddr_in address;
} client_info_t;

typedef struct
{
    char username[USERNAME_LEN];
    int socket_fd;
    int active;
} user_t;

typedef struct
{
    char name[ROOM_NAME_LEN];
    int members[MAX_USERS];
    int member_count;
    int active;
} room_t;

static user_t users[MAX_USERS];
static room_t rooms[MAX_ROOMS];
static int user_count = 0;
static volatile sig_atomic_t server_running = 1;
static int server_fd_global = -1;

static pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t rooms_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t send_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ------------------------------------------------------------------ */
static void server_shutdown_handler(int signal_number)
{
    (void)signal_number;
    server_running = 0;

    if (server_fd_global >= 0)
    {
        close(server_fd_global);
        server_fd_global = -1;
    }
}

static void print_server_banner(void)
{
    printf("\n============================================================\n");
    printf("                 NetMessenger Server\n");
    printf("============================================================\n");
    printf(" Status : RUNNING\n");
    printf(" Port   : %d\n", PORT);
    printf(" NID    : %04d\n", NID);
    printf(" Clients: up to %d\n", MAX_USERS);
    printf(" Storage: %s/<sender>/\n", STORAGE_ROOT);
    printf(" Log    : %s\n", LOG_FILE);
    printf("------------------------------------------------------------\n");
    printf(" Server console events: [START] [CONN] [REG] [CMD] [FILE]\n");
    printf("                        [DISC] [WARN] [STOP]\n");
    printf("============================================================\n\n");
    fflush(stdout);
}

static void log_event(const char *message)
{
    FILE *fp;
    time_t now;
    struct tm tm_now;
    char timestamp[64];

    pthread_mutex_lock(&log_mutex);

    fp = fopen(LOG_FILE, "a");
    if (fp != NULL)
    {
        now = time(NULL);
        if (localtime_r(&now, &tm_now) != NULL)
        {
            strftime(timestamp, sizeof(timestamp),
                     "%Y-%m-%d %H:%M:%S", &tm_now);
            fprintf(fp, "[%s] %s\n", timestamp, message);
        }
        fclose(fp);
    }

    pthread_mutex_unlock(&log_mutex);
}

/* ------------------------------------------------------------------ */
static int send_all_locked(int fd, const void *data, size_t length)
{
    const unsigned char *p = (const unsigned char *)data;

    while (length > 0)
    {
        ssize_t n = send(fd, p, length, MSG_NOSIGNAL);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;

        p += (size_t)n;
        length -= (size_t)n;
    }

    return 0;
}

static int send_bytes(int fd, const void *data, size_t length)
{
    int rc;
    pthread_mutex_lock(&send_mutex);
    rc = send_all_locked(fd, data, length);
    pthread_mutex_unlock(&send_mutex);
    return rc;
}

static int send_text(int fd, const char *text)
{
    return send_bytes(fd, text, strlen(text));
}

static void send_ok(int fd, const char *message)
{
    char response[LINE_SIZE];
    snprintf(response, sizeof(response), "OK %s NID:%d\n", message, NID);
    send_text(fd, response);
}

static void send_error(int fd, const char *code, const char *reason)
{
    char response[LINE_SIZE];
    snprintf(response, sizeof(response), "ERR %s %s NID:%d\n",
             code, reason, NID);
    send_text(fd, response);
}

/* ------------------------------------------------------------------
   Buffered line reader.
   Returns:
     >0 = line length (without newline)
      0 = clean EOF before any data
     -1 = error
     -2 = line too long
   ------------------------------------------------------------------ */
static int reader_refill(conn_reader_t *r)
{
    if (r->start < r->end)
        return 1;

    r->start = 0;
    r->end = 0;

    for (;;)
    {
        ssize_t n = recv(r->fd, r->buffer, sizeof(r->buffer), 0);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return 0;

        r->end = (size_t)n;
        return 1;
    }
}

static ssize_t reader_line(conn_reader_t *r, char *out, size_t out_size)
{
    size_t used = 0;

    if (out_size == 0)
        return -2;

    while (1)
    {
        size_t i;

        if (r->start == r->end)
        {
            int rc = reader_refill(r);
            if (rc <= 0)
            {
                if (rc == 0 && used == 0)
                    return 0;
                if (rc == 0)
                {
                    out[used] = '\0';
                    return (ssize_t)used;
                }
                return -1;
            }
        }

        for (i = r->start; i < r->end; ++i)
        {
            if (r->buffer[i] == '\n')
            {
                size_t count = i - r->start;

                if (used + count >= out_size)
                {
                    r->start = i + 1;
                    return -2;
                }

                memcpy(out + used, r->buffer + r->start, count);
                used += count;
                r->start = i + 1;

                while (used > 0 && out[used - 1] == '\r')
                    --used;

                out[used] = '\0';
                return (ssize_t)used;
            }
        }

        if (used + (r->end - r->start) >= out_size)
        {
            r->start = r->end;
            return -2;
        }

        memcpy(out + used, r->buffer + r->start, r->end - r->start);
        used += r->end - r->start;
        r->start = r->end;
    }
}

/* Read exactly length bytes, first consuming bytes already buffered. */
static int reader_exact(conn_reader_t *r, void *data, uint64_t length)
{
    unsigned char *p = (unsigned char *)data;

    while (length > 0)
    {
        if (r->start < r->end)
        {
            size_t available = r->end - r->start;
            size_t take = available;
            if ((uint64_t)take > length)
                take = (size_t)length;

            memcpy(p, r->buffer + r->start, take);
            r->start += take;
            p += take;
            length -= take;
            continue;
        }

        {
            size_t want = (length > IO_BUFFER_SIZE)
                        ? IO_BUFFER_SIZE
                        : (size_t)length;
            ssize_t n = recv(r->fd, p, want, 0);

            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                return -1;
            }
            if (n == 0)
                return -1;

            p += (size_t)n;
            length -= (uint64_t)n;
        }
    }

    return 0;
}

static int reader_discard(conn_reader_t *r, uint64_t length)
{
    unsigned char scratch[IO_BUFFER_SIZE];

    while (length > 0)
    {
        size_t chunk = (length > sizeof(scratch))
                     ? sizeof(scratch)
                     : (size_t)length;

        if (reader_exact(r, scratch, chunk) < 0)
            return -1;

        length -= (uint64_t)chunk;
    }

    return 0;
}

/* ------------------------------------------------------------------ */
static int find_user_by_name_locked(const char *name)
{
    int i;
    for (i = 0; i < MAX_USERS; ++i)
    {
        if (users[i].active && strcmp(users[i].username, name) == 0)
            return i;
    }
    return -1;
}

static int find_user_by_socket_locked(int fd)
{
    int i;
    for (i = 0; i < MAX_USERS; ++i)
    {
        if (users[i].active && users[i].socket_fd == fd)
            return i;
    }
    return -1;
}

static int register_user(const char *username, int fd)
{
    int i;
    int slot = -1;

    pthread_mutex_lock(&users_mutex);

    if (find_user_by_name_locked(username) >= 0)
    {
        pthread_mutex_unlock(&users_mutex);
        return -1;
    }

    for (i = 0; i < MAX_USERS; ++i)
    {
        if (!users[i].active)
        {
            slot = i;
            break;
        }
    }

    if (slot < 0)
    {
        pthread_mutex_unlock(&users_mutex);
        return -2;
    }

    memset(&users[slot], 0, sizeof(users[slot]));
    strncpy(users[slot].username, username, USERNAME_LEN - 1);
    users[slot].socket_fd = fd;
    users[slot].active = 1;
    ++user_count;

    pthread_mutex_unlock(&users_mutex);
    return 0;
}

static int remove_user(int fd, char *username, size_t username_size)
{
    int index;

    pthread_mutex_lock(&users_mutex);
    index = find_user_by_socket_locked(fd);

    if (index < 0)
    {
        pthread_mutex_unlock(&users_mutex);
        return 0;
    }

    if (username_size > 0)
    {
        strncpy(username, users[index].username, username_size - 1);
        username[username_size - 1] = '\0';
    }

    users[index].active = 0;
    users[index].socket_fd = -1;
    memset(users[index].username, 0, sizeof(users[index].username));
    if (user_count > 0)
        --user_count;

    pthread_mutex_unlock(&users_mutex);
    return 1;
}

/* ------------------------------------------------------------------ */
static void notify_users(const char *message, int exclude_fd)
{
    int sockets[MAX_USERS];
    int count = 0;
    int i;

    pthread_mutex_lock(&users_mutex);
    for (i = 0; i < MAX_USERS && count < MAX_USERS; ++i)
    {
        if (users[i].active && users[i].socket_fd != exclude_fd)
            sockets[count++] = users[i].socket_fd;
    }
    pthread_mutex_unlock(&users_mutex);

    for (i = 0; i < count; ++i)
        send_text(sockets[i], message);
}

static void send_user_list(int fd)
{
    char response[LINE_SIZE];
    size_t used = 0;
    int first = 1;
    int i;

    used += (size_t)snprintf(response + used, sizeof(response) - used,
                             "OK USERS ");

    pthread_mutex_lock(&users_mutex);
    for (i = 0; i < MAX_USERS; ++i)
    {
        if (users[i].active)
        {
            int n = snprintf(response + used, sizeof(response) - used,
                             "%s%s", first ? "" : ",", users[i].username);
            if (n < 0 || (size_t)n >= sizeof(response) - used)
                break;
            used += (size_t)n;
            first = 0;
        }
    }
    pthread_mutex_unlock(&users_mutex);

    snprintf(response + used, sizeof(response) - used, " NID:%d\n", NID);
    send_text(fd, response);
}

/* ------------------------------------------------------------------ */
static int find_room_locked(const char *name)
{
    int i;
    for (i = 0; i < MAX_ROOMS; ++i)
    {
        if (rooms[i].active && strcmp(rooms[i].name, name) == 0)
            return i;
    }
    return -1;
}

static int room_has_member_locked(const room_t *room, int fd)
{
    int i;
    for (i = 0; i < room->member_count; ++i)
    {
        if (room->members[i] == fd)
            return 1;
    }
    return 0;
}

static int add_room_member_locked(room_t *room, int fd)
{
    if (room_has_member_locked(room, fd))
        return 1;

    if (room->member_count >= MAX_USERS)
        return 0;

    room->members[room->member_count++] = fd;
    return 1;
}

static void remove_room_member_locked(room_t *room, int fd)
{
    int i;
    for (i = 0; i < room->member_count; ++i)
    {
        if (room->members[i] == fd)
        {
            int j;
            for (j = i; j < room->member_count - 1; ++j)
                room->members[j] = room->members[j + 1];
            --room->member_count;
            return;
        }
    }
}

static void remove_user_from_all_rooms(int fd)
{
    int i;

    pthread_mutex_lock(&rooms_mutex);
    for (i = 0; i < MAX_ROOMS; ++i)
    {
        if (rooms[i].active)
        {
            remove_room_member_locked(&rooms[i], fd);
            if (rooms[i].member_count == 0)
                memset(&rooms[i], 0, sizeof(rooms[i]));
        }
    }
    pthread_mutex_unlock(&rooms_mutex);
}

static void send_room_list(int fd)
{
    char response[LINE_SIZE];
    size_t used = 0;
    int first = 1;
    int i;

    used += (size_t)snprintf(response + used, sizeof(response) - used,
                             "OK ROOMS ");

    pthread_mutex_lock(&rooms_mutex);
    for (i = 0; i < MAX_ROOMS; ++i)
    {
        if (rooms[i].active)
        {
            int n = snprintf(response + used, sizeof(response) - used,
                             "%s%s", first ? "" : ",", rooms[i].name);
            if (n < 0 || (size_t)n >= sizeof(response) - used)
                break;
            used += (size_t)n;
            first = 0;
        }
    }
    pthread_mutex_unlock(&rooms_mutex);

    snprintf(response + used, sizeof(response) - used, " NID:%d\n", NID);
    send_text(fd, response);
}

/* ------------------------------------------------------------------ */
static void handle_join(int fd, const char *username, const char *command)
{
    char room_name[ROOM_NAME_LEN];
    int index = -1;
    int created = 0;

    if (sscanf(command, "JOIN %49s", room_name) != 1)
    {
        send_error(fd, "003", "INVALID_ROOM_FORMAT");
        return;
    }

    pthread_mutex_lock(&rooms_mutex);

    index = find_room_locked(room_name);
    if (index < 0)
    {
        int i;
        for (i = 0; i < MAX_ROOMS; ++i)
        {
            if (!rooms[i].active)
            {
                index = i;
                memset(&rooms[i], 0, sizeof(rooms[i]));
                strncpy(rooms[i].name, room_name, ROOM_NAME_LEN - 1);
                rooms[i].active = 1;
                created = 1;
                break;
            }
        }
    }

    if (index >= 0 && add_room_member_locked(&rooms[index], fd))
    {
        pthread_mutex_unlock(&rooms_mutex);

        {
            char response[LINE_SIZE];
            snprintf(response, sizeof(response), "JOINED %s", room_name);
            send_ok(fd, response);
        }

        {
            char log_message[LINE_SIZE];
            snprintf(log_message, sizeof(log_message),
                     "User %s joined room %s%s",
                     username, room_name, created ? " (created)" : "");
            log_event(log_message);
        }
        return;
    }

    pthread_mutex_unlock(&rooms_mutex);
    send_error(fd, "003", "ROOM_FULL");
}

static void handle_leave(int fd, const char *username, const char *command)
{
    char room_name[ROOM_NAME_LEN];
    int index;
    int existed;

    if (sscanf(command, "LEAVE %49s", room_name) != 1)
    {
        send_error(fd, "003", "ROOM_NOT_FOUND");
        return;
    }

    pthread_mutex_lock(&rooms_mutex);
    index = find_room_locked(room_name);
    existed = (index >= 0);

    if (index >= 0)
    {
        remove_room_member_locked(&rooms[index], fd);
        if (rooms[index].member_count == 0)
            memset(&rooms[index], 0, sizeof(rooms[index]));
    }
    pthread_mutex_unlock(&rooms_mutex);

    if (!existed)
    {
        send_error(fd, "003", "ROOM_NOT_FOUND");
        return;
    }

    {
        char response[LINE_SIZE];
        snprintf(response, sizeof(response), "LEFT %s", room_name);
        send_ok(fd, response);
    }

    {
        char log_message[LINE_SIZE];
        snprintf(log_message, sizeof(log_message),
                 "User %s left room %s", username, room_name);
        log_event(log_message);
    }
}

static void handle_room_message(int fd, const char *sender, const char *command)
{
    char room_name[ROOM_NAME_LEN];
    const char *message;
    int index;
    int member = 0;
    int sockets[MAX_USERS];
    int count = 0;
    int i;
    char outgoing[LINE_SIZE];

    if (sscanf(command, "RMSG %49s", room_name) != 1)
    {
        send_error(fd, "003", "ROOM_NOT_FOUND");
        return;
    }

    message = command + 5;
    while (*message && *message != ' ')
        ++message;
    while (*message == ' ')
        ++message;

    if (*message == '\0')
    {
        send_error(fd, "003", "INVALID_MESSAGE_FORMAT");
        return;
    }

    pthread_mutex_lock(&rooms_mutex);
    index = find_room_locked(room_name);
    if (index >= 0)
    {
        member = room_has_member_locked(&rooms[index], fd);
        if (member)
        {
            for (i = 0; i < rooms[index].member_count && count < MAX_USERS; ++i)
                sockets[count++] = rooms[index].members[i];
        }
    }
    pthread_mutex_unlock(&rooms_mutex);

    if (index < 0)
    {
        send_error(fd, "003", "ROOM_NOT_FOUND");
        return;
    }

    if (!member)
    {
        send_error(fd, "003", "NOT_IN_ROOM");
        return;
    }

    snprintf(outgoing, sizeof(outgoing), "MSG ROOM %s %s %s\n",
             room_name, sender, message);

    for (i = 0; i < count; ++i)
        send_text(sockets[i], outgoing);

    send_ok(fd, "SENT");

    {
        char log_message[LINE_SIZE];
        snprintf(log_message, sizeof(log_message),
                 "RMSG %s -> %s : %s", sender, room_name, message);
        log_event(log_message);
    }
}

/* ------------------------------------------------------------------ */
static void handle_bcast(int fd, const char *sender, const char *command)
{
    const char *message = command + 6;
    int sockets[MAX_USERS];
    int count = 0;
    int i;
    char outgoing[LINE_SIZE];

    while (*message == ' ')
        ++message;

    if (*message == '\0')
    {
        send_error(fd, "002", "INVALID_MESSAGE_FORMAT");
        return;
    }

    pthread_mutex_lock(&users_mutex);
    for (i = 0; i < MAX_USERS && count < MAX_USERS; ++i)
    {
        if (users[i].active && users[i].socket_fd != fd)
            sockets[count++] = users[i].socket_fd;
    }
    pthread_mutex_unlock(&users_mutex);

    snprintf(outgoing, sizeof(outgoing), "MSG BCAST %s %s\n", sender, message);
    for (i = 0; i < count; ++i)
        send_text(sockets[i], outgoing);

    send_ok(fd, "SENT");

    {
        char log_message[LINE_SIZE];
        snprintf(log_message, sizeof(log_message), "BCAST %s : %s", sender, message);
        log_event(log_message);
    }
}

static void handle_pmsg(int fd, const char *sender, const char *command)
{
    char target[USERNAME_LEN];
    int target_fd = -1;
    const char *message;
    char outgoing[LINE_SIZE];

    if (sscanf(command, "PMSG %49s", target) != 1)
    {
        send_error(fd, "002", "INVALID_MESSAGE_FORMAT");
        return;
    }

    message = command + 5;
    while (*message && *message != ' ')
        ++message;
    while (*message == ' ')
        ++message;

    if (*message == '\0')
    {
        send_error(fd, "002", "INVALID_MESSAGE_FORMAT");
        return;
    }

    pthread_mutex_lock(&users_mutex);
    {
        int index = find_user_by_name_locked(target);
        if (index >= 0)
            target_fd = users[index].socket_fd;
    }
    pthread_mutex_unlock(&users_mutex);

    if (target_fd < 0)
    {
        send_error(fd, "002", "USER_NOT_FOUND");
        return;
    }

    snprintf(outgoing, sizeof(outgoing), "MSG PRIV %s %s\n", sender, message);
    if (send_text(target_fd, outgoing) < 0)
    {
        send_error(fd, "002", "USER_NOT_FOUND");
        return;
    }

    send_ok(fd, "SENT");

    {
        char log_message[LINE_SIZE];
        snprintf(log_message, sizeof(log_message), "PMSG %s -> %s : %s",
                 sender, target, message);
        log_event(log_message);
    }
}

/* ------------------------------------------------------------------ */
static int extract_safe_filename(const char *input, char *output, size_t output_size)
{
    const char *base;
    size_t length;

    if (input == NULL || output == NULL || output_size < 2)
        return -1;

    if (input[0] == '\0' || strcmp(input, ".") == 0 || strcmp(input, "..") == 0)
        return -1;

    if (strstr(input, "..") != NULL)
        return -1;

    if (strchr(input, '/') != NULL || strchr(input, '\\') != NULL)
        base = strrchr(input, '/') ? strrchr(input, '/') + 1 : strrchr(input, '\\') + 1;
    else
        base = input;

    if (*base == '\0')
        return -1;

    length = strlen(base);
    if (length >= output_size)
        return -1;

    if (base[0] == '.')
        return -1;

    strncpy(output, base, output_size - 1);
    output[output_size - 1] = '\0';
    return 0;
}

static int mkdir_if_needed(const char *path)
{
    struct stat st;

    if (mkdir(path, 0755) == 0)
        return 0;

    if (errno == EEXIST && stat(path, &st) == 0 && S_ISDIR(st.st_mode))
        return 0;

    return -1;
}

static int prepare_storage(const char *sender, char *directory, size_t directory_size)
{
    char path[512];

    if (mkdir_if_needed("storage") < 0)
        return -1;
    if (mkdir_if_needed(STORAGE_ROOT) < 0)
        return -1;

    snprintf(path, sizeof(path), "%s/%s", STORAGE_ROOT, sender);
    if (mkdir_if_needed(path) < 0)
        return -1;

    strncpy(directory, path, directory_size - 1);
    directory[directory_size - 1] = '\0';
    return 0;
}

static int receive_file_to_disk(conn_reader_t *reader, const char *path, uint64_t size)
{
    FILE *fp = fopen(path, "wb");
    unsigned char buffer[IO_BUFFER_SIZE];
    uint64_t remaining = size;

    if (fp == NULL)
        return -1;

    while (remaining > 0)
    {
        size_t chunk = remaining > sizeof(buffer) ? sizeof(buffer) : (size_t)remaining;

        if (reader_exact(reader, buffer, chunk) < 0)
        {
            fclose(fp);
            return -1;
        }

        if (fwrite(buffer, 1, chunk, fp) != chunk)
        {
            fclose(fp);
            return -1;
        }

        remaining -= (uint64_t)chunk;
    }

    if (fclose(fp) != 0)
        return -1;

    return 0;
}

static int send_file_to_socket(int target_fd, const char *sender,
                               const char *filename, const char *path,
                               uint64_t size)
{
    FILE *fp;
    unsigned char buffer[IO_BUFFER_SIZE];
    uint64_t remaining = size;
    char header[LINE_SIZE];
    int rc = 0;

    fp = fopen(path, "rb");
    if (fp == NULL)
        return -1;

    snprintf(header, sizeof(header), "FILE %s %s %" PRIu64 "\n",
             sender, filename, size);

    pthread_mutex_lock(&send_mutex);

    if (send_all_locked(target_fd, header, strlen(header)) < 0)
    {
        rc = -1;
        goto done;
    }

    while (remaining > 0)
    {
        size_t chunk = remaining > sizeof(buffer) ? sizeof(buffer) : (size_t)remaining;
        size_t got = fread(buffer, 1, chunk, fp);

        if (got != chunk)
        {
            rc = -1;
            goto done;
        }

        if (send_all_locked(target_fd, buffer, got) < 0)
        {
            rc = -1;
            goto done;
        }

        remaining -= (uint64_t)got;
    }

done:
    pthread_mutex_unlock(&send_mutex);
    fclose(fp);
    return rc;
}

static void handle_sendfile(int fd, const char *sender,
                            conn_reader_t *reader, const char *command)
{
    char target[MAX_TARGET_LEN];
    char input_filename[MAX_FILENAME_LEN];
    char filename[MAX_FILENAME_LEN];
    unsigned long long parsed_size = 0;
    uint64_t file_size;
    int target_fd = -1;
    int room_index = -1;
    int target_is_room = 0;
    int room_sockets[MAX_USERS];
    int room_socket_count = 0;
    char sender_dir[512];
    char file_path[1024];
    int i;

    {
        char extra[2];

        if (sscanf(command,
                   "SENDFILE %49s %255s %llu %1s",
                   target,
                   input_filename,
                   &parsed_size,
                   extra) != 3)
        {
            send_error(fd, "003", "INVALID_FILE_FORMAT");
            log_event("Rejected malformed SENDFILE command");
            return;
        }
    }

    file_size = (uint64_t)parsed_size;

    if (file_size > MAX_FILE_SIZE)
    {
        if (reader_discard(reader, file_size) < 0)
            return;
        send_error(fd, "004", "FILE_TOO_LARGE");
        log_event("Rejected SENDFILE: file too large");
        return;
    }

    if (extract_safe_filename(input_filename, filename, sizeof(filename)) < 0)
    {
        if (reader_discard(reader, file_size) < 0)
            return;
        send_error(fd, "003", "INVALID_FILENAME");
        log_event("Rejected SENDFILE: invalid filename");
        return;
    }

    pthread_mutex_lock(&users_mutex);
    i = find_user_by_name_locked(target);
    if (i >= 0)
        target_fd = users[i].socket_fd;
    pthread_mutex_unlock(&users_mutex);

    if (target_fd < 0)
    {
        pthread_mutex_lock(&rooms_mutex);
        room_index = find_room_locked(target);
        if (room_index >= 0)
        {
            target_is_room = 1;
            for (i = 0; i < rooms[room_index].member_count && room_socket_count < MAX_USERS; ++i)
            {
                if (rooms[room_index].members[i] != fd)
                    room_sockets[room_socket_count++] = rooms[room_index].members[i];
            }
        }
        pthread_mutex_unlock(&rooms_mutex);
    }

    if (target_fd < 0 && !target_is_room)
    {
        if (reader_discard(reader, file_size) < 0)
            return;
        send_error(fd, "002", "USER_NOT_FOUND");
        return;
    }

    if (prepare_storage(sender, sender_dir, sizeof(sender_dir)) < 0)
    {
        if (reader_discard(reader, file_size) < 0)
            return;
        send_error(fd, "005", "STORAGE_ERROR");
        return;
    }

    snprintf(file_path, sizeof(file_path), "%s/%s", sender_dir, filename);

    if (receive_file_to_disk(reader, file_path, file_size) < 0)
    {
        send_error(fd, "005", "FILE_SAVE_ERROR");
        return;
    }

    if (target_fd >= 0)
    {
        if (send_file_to_socket(target_fd, sender, filename, file_path, file_size) < 0)
            log_event("File delivery to user failed");
    }

    if (target_is_room)
    {
        for (i = 0; i < room_socket_count; ++i)
        {
            if (send_file_to_socket(room_sockets[i], sender, filename,
                                    file_path, file_size) < 0)
                log_event("File delivery to room member failed");
        }
    }

    {
        char response[LINE_SIZE];
        snprintf(response, sizeof(response), "FILE_RECEIVED %s", filename);
        send_ok(fd, response);
    }

    {
        char log_message[LINE_SIZE];
        snprintf(log_message, sizeof(log_message),
                 "SENDFILE %s -> %s : %s (%" PRIu64 " bytes)",
                 sender, target, filename, file_size);
        log_event(log_message);
        printf("[FILE] %s -> %s | %s | %" PRIu64 " bytes\n",
               sender,
               target,
               filename,
               file_size);
    }
}

/* ------------------------------------------------------------------ */
static void *client_thread(void *arg)
{
    client_info_t *client = (client_info_t *)arg;
    int fd = client->socket_fd;
    char client_ip[INET_ADDRSTRLEN];
    char line[LINE_SIZE];
    char username[USERNAME_LEN] = "";
    int registered = 0;
    conn_reader_t reader;
    char log_message[LINE_SIZE];

    if (inet_ntop(AF_INET, &client->address.sin_addr,
                  client_ip, sizeof(client_ip)) == NULL)
        strcpy(client_ip, "Unknown");

    snprintf(log_message, sizeof(log_message),
             "Connection from %s:%d", client_ip,
             ntohs(client->address.sin_port));
    log_event(log_message);

    printf("[CONN] Client connected from %s:%d\n",
           client_ip,
           ntohs(client->address.sin_port));

    free(client);

    reader.fd = fd;
    reader.start = 0;
    reader.end = 0;

    send_text(fd, "WELCOME NetMessenger\n");

    while (1)
    {
        ssize_t n = reader_line(&reader, line, sizeof(line));

        if (n == 0)
            break;

        if (n == -2)
        {
            send_error(fd, "002", "LINE_TOO_LONG");
            continue;
        }

        if (n < 0)
        {
            if (errno != ECONNRESET)
                perror("recv");
            break;
        }

        if (line[0] == '\0')
            continue;

        if (registered)
            printf("[CMD ] %-16s %s\n", username, line);
        else
            printf("[CMD ] <unregistered>  %s\n", line);

        /* REGISTER must be the first command. */
        if (strncmp(line, "REGISTER ", 9) == 0)
        {
            char requested[USERNAME_LEN];
            int rc;

            if (registered)
            {
                send_error(fd, "001", "ALREADY_REGISTERED");
                continue;
            }

            if (sscanf(line, "REGISTER %49s", requested) != 1 || requested[0] == '\0')
            {
                send_error(fd, "001", "INVALID_USERNAME");
                continue;
            }

            rc = register_user(requested, fd);
            if (rc == 0)
            {
                strncpy(username, requested, USERNAME_LEN - 1);
                username[USERNAME_LEN - 1] = '\0';
                registered = 1;

                {
                    char response[LINE_SIZE];
                    snprintf(response, sizeof(response), "REGISTERED %s", username);
                    send_ok(fd, response);
                }

                snprintf(log_message, sizeof(log_message),
                         "User registered: %s", username);
                printf("[REG ] %-16s registered successfully\n", username);
                log_event(log_message);

                {
                    char notification[LINE_SIZE];
                    snprintf(notification, sizeof(notification),
                             "MSG JOIN %s\n", username);
                    notify_users(notification, fd);
                }
            }
            else if (rc == -1)
            {
                send_error(fd, "001", "USERNAME_TAKEN");
            }
            else
            {
                send_error(fd, "005", "SERVER_FULL");
            }
            continue;
        }

        /* Any command other than REGISTER before registration is invalid. */
        if (!registered)
        {
            if (strncmp(line, "SENDFILE ", 9) == 0)
            {
                char target[MAX_TARGET_LEN];
                char filename[MAX_FILENAME_LEN];
                unsigned long long size = 0;

                if (sscanf(line, "SENDFILE %49s %255s %llu",
                           target, filename, &size) == 3)
                    reader_discard(&reader, (uint64_t)size);
            }

            send_error(fd, "002", "REGISTER_REQUIRED");
            continue;
        }

        if (strcmp(line, "LIST") == 0)
        {
            send_user_list(fd);
            continue;
        }

        if (strcmp(line, "BCAST") == 0 || strncmp(line, "BCAST ", 6) == 0)
        {
            if (strcmp(line, "BCAST") == 0)
                send_error(fd, "002", "INVALID_MESSAGE_FORMAT");
            else
                handle_bcast(fd, username, line);
            continue;
        }

        if (strcmp(line, "PMSG") == 0 || strncmp(line, "PMSG ", 5) == 0)
        {
            if (strcmp(line, "PMSG") == 0)
                send_error(fd, "002", "INVALID_MESSAGE_FORMAT");
            else
                handle_pmsg(fd, username, line);
            continue;
        }

        if (strcmp(line, "JOIN") == 0 || strncmp(line, "JOIN ", 5) == 0)
        {
            if (strcmp(line, "JOIN") == 0)
                send_error(fd, "003", "INVALID_ROOM_FORMAT");
            else
                handle_join(fd, username, line);
            continue;
        }

        if (strcmp(line, "LEAVE") == 0 || strncmp(line, "LEAVE ", 6) == 0)
        {
            if (strcmp(line, "LEAVE") == 0)
                send_error(fd, "003", "ROOM_NOT_FOUND");
            else
                handle_leave(fd, username, line);
            continue;
        }

        if (strcmp(line, "ROOMS") == 0)
        {
            send_room_list(fd);
            continue;
        }

        if (strcmp(line, "RMSG") == 0 || strncmp(line, "RMSG ", 5) == 0)
        {
            if (strcmp(line, "RMSG") == 0)
                send_error(fd, "003", "INVALID_MESSAGE_FORMAT");
            else
                handle_room_message(fd, username, line);
            continue;
        }

        if (strncmp(line, "SENDFILE ", 9) == 0)
        {
            handle_sendfile(fd, username, &reader, line);
            continue;
        }

        if (strcmp(line, "QUIT") == 0)
        {
            send_ok(fd, "BYE");
            break;
        }

        send_error(fd, "002", "UNKNOWN_COMMAND");
    }

    if (registered)
    {
        char disconnected[USERNAME_LEN] = "";
        if (remove_user(fd, disconnected, sizeof(disconnected)))
        {
            remove_user_from_all_rooms(fd);

            snprintf(log_message, sizeof(log_message),
                     "User disconnected: %s", disconnected);
            log_event(log_message);

            {
                char notification[LINE_SIZE];
                snprintf(notification, sizeof(notification),
                         "MSG LEAVE %s\n", disconnected);
                notify_users(notification, fd);
            }
        }
    }

    close(fd);
    printf("Client connection closed.\n");
    return NULL;
}

/* ------------------------------------------------------------------ */
int main(void)
{
    int server_fd;
    int opt = 1;
    struct sockaddr_in server_address;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, server_shutdown_handler);
    signal(SIGTERM, server_shutdown_handler);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    server_fd_global = server_fd;

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt SO_REUSEADDR");
        close(server_fd);
        server_fd_global = -1;
        return EXIT_FAILURE;
    }

    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&server_address,
             sizeof(server_address)) < 0)
    {
        perror("bind");
        close(server_fd);
        server_fd_global = -1;
        return EXIT_FAILURE;
    }

    if (listen(server_fd, BACKLOG) < 0)
    {
        perror("listen");
        close(server_fd);
        server_fd_global = -1;
        return EXIT_FAILURE;
    }

    print_server_banner();
    printf("[START] Listening for TCP clients on port %d...\n", PORT);
    log_event("Server started on port 6826");

    while (server_running)
    {
        client_info_t *client;
        socklen_t address_length;
        pthread_t thread;

        client = malloc(sizeof(*client));
        if (client == NULL)
        {
            fprintf(stderr, "[WARN] malloc failed; waiting for next client.\n");
            continue;
        }

        memset(client, 0, sizeof(*client));
        address_length = sizeof(client->address);

        client->socket_fd = accept(server_fd,
                                    (struct sockaddr *)&client->address,
                                    &address_length);

        if (client->socket_fd < 0)
        {
            if (!server_running)
            {
                free(client);
                break;
            }

            if (errno == EINTR)
            {
                free(client);
                continue;
            }

            perror("[WARN] accept");
            free(client);
            continue;
        }

        if (pthread_create(&thread, NULL, client_thread, client) != 0)
        {
            perror("[WARN] pthread_create");
            close(client->socket_fd);
            free(client);
            continue;
        }

        pthread_detach(thread);
    }

    if (server_fd_global >= 0)
    {
        close(server_fd_global);
        server_fd_global = -1;
    }

    log_event("Server stopped");
    printf("\n============================================================\n");
    printf("[STOP ] NetMessenger server stopped cleanly.\n");
    printf("============================================================\n");

    return EXIT_SUCCESS;
}
