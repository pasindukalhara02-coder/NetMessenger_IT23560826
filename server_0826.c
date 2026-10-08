/*
 * NetMessenger - IE3010 Network Programming
 * Student: Pasindu Kalhara
 * Registration Number: IT23560826
 *
 * Personalisation:
 *   Port: 6826
 *   NID : NID:5608
 *
 * Implemented features:
 *   - TCP server
 *   - Thread-per-client concurrency
 *   - REGISTER
 *   - LIST
 *   - BCAST
 *   - JOIN/LEAVE presence notifications
 *   - QUIT
 *   - TCP line framing
 *   - Error handling foundation
 *   - Server logging
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define SERVER_PORT 6826
#define BACKLOG 10
#define BUFFER_SIZE 1024
#define MAX_CLIENTS 100
#define MAX_USERNAME 64

#define LOG_FILE "netmsg_IT23560826.log"
#define NID_TAG "NID:5608"

typedef struct {
    int socket_fd;
    int active;
    int registered;
    char username[MAX_USERNAME];
} ClientInfo;

static volatile sig_atomic_t server_running = 1;
static int listen_fd = -1;

static ClientInfo clients[MAX_CLIENTS];

static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
 * Signal handling
 * ========================================================= */
static void handle_signal(int signal_number)
{
    (void)signal_number;

    server_running = 0;

    if (listen_fd != -1) {
        close(listen_fd);
        listen_fd = -1;
    }
}


/* =========================================================
 * Server logging
 * ========================================================= */
static void log_event(const char *event)
{
    FILE *log_file;
    time_t now;
    struct tm time_info;
    char timestamp[64];

    now = time(NULL);

    if (localtime_r(&now, &time_info) == NULL) {
        return;
    }

    if (strftime(timestamp,
                 sizeof(timestamp),
                 "%Y-%m-%d %H:%M:%S",
                 &time_info) == 0) {
        return;
    }

    pthread_mutex_lock(&log_mutex);

    log_file = fopen(LOG_FILE, "a");

    if (log_file != NULL) {
        fprintf(log_file,
                "[%s] %s\n",
                timestamp,
                event);

        fclose(log_file);
    }

    pthread_mutex_unlock(&log_mutex);
}


/* =========================================================
 * Reliable send
 * ========================================================= */
static ssize_t send_all(int socket_fd,
                        const void *data,
                        size_t length)
{
    const char *buffer = (const char *)data;
    size_t total_sent = 0;

    while (total_sent < length) {

        ssize_t sent = send(socket_fd,
                            buffer + total_sent,
                            length - total_sent,
                            MSG_NOSIGNAL);

        if (sent > 0) {
            total_sent += (size_t)sent;
            continue;
        }

        if (sent == -1 && errno == EINTR) {
            continue;
        }

        return -1;
    }

    return (ssize_t)total_sent;
}


/* =========================================================
 * Send one protocol response
 * ========================================================= */
static int send_response(int socket_fd,
                         const char *format,
                         ...)
{
    char response[BUFFER_SIZE];
    int length;
    va_list args;

    va_start(args, format);

    length = vsnprintf(response,
                       sizeof(response),
                       format,
                       args);

    va_end(args);

    if (length < 0 ||
        (size_t)length >= sizeof(response)) {
        return -1;
    }

    return (int)send_all(socket_fd,
                          response,
                          (size_t)length);
}


/* =========================================================
 * Receive one complete text line
 *
 * Handles TCP fragmentation by reading one byte at a time.
 * It stops exactly at '\n', leaving following bytes unread.
 * This is important for future SENDFILE raw-byte handling.
 * ========================================================= */
static int recv_line(int socket_fd,
                     char *buffer,
                     size_t buffer_size)
{
    size_t position = 0;

    if (buffer_size < 2) {
        return -1;
    }

    while (1) {

        char ch;

        ssize_t received = recv(socket_fd,
                                &ch,
                                1,
                                0);

        if (received == 1) {

            if (ch == '\n') {
                buffer[position] = '\0';
                return 1;
            }

            if (position + 1 < buffer_size) {
                buffer[position++] = ch;
            } else {

                do {
                    received = recv(socket_fd,
                                    &ch,
                                    1,
                                    0);

                    if (received <= 0) {
                        break;
                    }

                } while (ch != '\n');

                buffer[buffer_size - 1] = '\0';

                return -2;
            }

            continue;
        }

        if (received == 0) {
            return 0;
        }

        if (errno == EINTR) {
            continue;
        }

        return -1;
    }
}


/* =========================================================
 * Find registered user
 *
 * Caller must hold clients_mutex.
 * ========================================================= */
static int find_registered_user(const char *username)
{
    int index;

    for (index = 0; index < MAX_CLIENTS; index++) {

        if (clients[index].active &&
            clients[index].registered &&
            strcmp(clients[index].username, username) == 0) {

            return index;
        }
    }

    return -1;
}


/* =========================================================
 * Allocate client slot
 * ========================================================= */
static int allocate_client_slot(int socket_fd)
{
    int index;

    pthread_mutex_lock(&clients_mutex);

    for (index = 0; index < MAX_CLIENTS; index++) {

        if (!clients[index].active) {

            clients[index].socket_fd = socket_fd;
            clients[index].active = 1;
            clients[index].registered = 0;
            clients[index].username[0] = '\0';

            pthread_mutex_unlock(&clients_mutex);

            return index;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    return -1;
}


/* =========================================================
 * Username validation
 * ========================================================= */
static int validate_username(const char *username)
{
    size_t index;

    if (username == NULL ||
        username[0] == '\0' ||
        strlen(username) >= MAX_USERNAME) {

        return 0;
    }

    for (index = 0;
         username[index] != '\0';
         index++) {

        if (isspace((unsigned char)username[index])) {
            return 0;
        }
    }

    return 1;
}


/* =========================================================
 * REGISTER
 * ========================================================= */
static int register_client(int slot,
                           const char *username)
{
    char log_message[BUFFER_SIZE];

    if (!validate_username(username)) {
        return 0;
    }

    pthread_mutex_lock(&clients_mutex);

    if (clients[slot].registered ||
        find_registered_user(username) != -1) {

        pthread_mutex_unlock(&clients_mutex);

        return -1;
    }

    strncpy(clients[slot].username,
            username,
            sizeof(clients[slot].username) - 1);

    clients[slot].username[
        sizeof(clients[slot].username) - 1
    ] = '\0';

    clients[slot].registered = 1;

    snprintf(log_message,
             sizeof(log_message),
             "REGISTER %s",
             clients[slot].username);

    pthread_mutex_unlock(&clients_mutex);

    log_event(log_message);

    return 1;
}


/* =========================================================
 * JOIN notification
 *
 * All other registered users receive:
 * MSG JOIN <username>
 * ========================================================= */
static void send_join_notification(int joining_slot,
                                   const char *username)
{
    char message[BUFFER_SIZE];
    int index;

    snprintf(message,
             sizeof(message),
             "MSG JOIN %s\n",
             username);

    pthread_mutex_lock(&clients_mutex);

    for (index = 0; index < MAX_CLIENTS; index++) {

        if (index != joining_slot &&
            clients[index].active &&
            clients[index].registered) {

            (void)send_all(clients[index].socket_fd,
                            message,
                            strlen(message));
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}


/* =========================================================
 * LEAVE notification
 *
 * Remaining registered users receive:
 * MSG LEAVE <username>
 * ========================================================= */
static void send_leave_notification(int leaving_slot,
                                    const char *username)
{
    char message[BUFFER_SIZE];
    int index;

    snprintf(message,
             sizeof(message),
             "MSG LEAVE %s\n",
             username);

    pthread_mutex_lock(&clients_mutex);

    for (index = 0; index < MAX_CLIENTS; index++) {

        if (index != leaving_slot &&
            clients[index].active &&
            clients[index].registered) {

            (void)send_all(clients[index].socket_fd,
                            message,
                            strlen(message));
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}


/* =========================================================
 * LIST
 *
 * Response:
 * OK USERS <comma-separated-users> NID:5608
 * ========================================================= */
static void handle_list(int socket_fd)
{
    char response[BUFFER_SIZE];
    size_t used;
    int first = 1;
    int index;

    used = (size_t)snprintf(response,
                            sizeof(response),
                            "OK USERS ");

    pthread_mutex_lock(&clients_mutex);

    for (index = 0; index < MAX_CLIENTS; index++) {

        if (clients[index].active &&
            clients[index].registered) {

            int written = snprintf(
                response + used,
                sizeof(response) - used,
                "%s%s",
                first ? "" : ",",
                clients[index].username
            );

            if (written < 0 ||
                (size_t)written >= sizeof(response) - used) {

                pthread_mutex_unlock(&clients_mutex);

                (void)send_response(
                    socket_fd,
                    "ERR 007 RESPONSE_TOO_LARGE "
                    NID_TAG "\n"
                );

                log_event(
                    "ERR 007 RESPONSE_TOO_LARGE"
                );

                return;
            }

            used += (size_t)written;
            first = 0;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    if (used + strlen(" " NID_TAG "\n") >=
        sizeof(response)) {

        (void)send_response(
            socket_fd,
            "ERR 007 RESPONSE_TOO_LARGE "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 RESPONSE_TOO_LARGE"
        );

        return;
    }

    snprintf(response + used,
             sizeof(response) - used,
             " " NID_TAG "\n");

    (void)send_all(socket_fd,
                    response,
                    strlen(response));

    log_event("LIST requested");
}


/* =========================================================
 * BCAST
 *
 * Sender:
 *   OK SENT NID:5608
 *
 * Other registered clients:
 *   MSG BCAST <sender> <message>
 * ========================================================= */
static void handle_broadcast(int sender_slot,
                            const char *message)
{
    char sender[MAX_USERNAME];
    char output[BUFFER_SIZE];
    char log_message[BUFFER_SIZE];

    int recipient_fds[MAX_CLIENTS];
    int recipient_count = 0;

    int index;

    pthread_mutex_lock(&clients_mutex);

    if (!clients[sender_slot].active ||
        !clients[sender_slot].registered) {

        pthread_mutex_unlock(&clients_mutex);

        return;
    }

    strncpy(sender,
            clients[sender_slot].username,
            sizeof(sender) - 1);

    sender[sizeof(sender) - 1] = '\0';

    for (index = 0; index < MAX_CLIENTS; index++) {

        if (index != sender_slot &&
            clients[index].active &&
            clients[index].registered) {

            recipient_fds[recipient_count++] =
                clients[index].socket_fd;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    if (snprintf(output,
                 sizeof(output),
                 "MSG BCAST %s %s\n",
                 sender,
                 message) >=
        (int)sizeof(output)) {

        log_event("BCAST message too large");

        return;
    }

    for (index = 0;
         index < recipient_count;
         index++) {

        (void)send_all(
            recipient_fds[index],
            output,
            strlen(output)
        );
    }

    snprintf(log_message,
             sizeof(log_message),
             "BCAST %s %s",
             sender,
             message);

    log_event(log_message);
}


/* =========================================================
 * Client cleanup
 * ========================================================= */
static void cleanup_client(int slot)
{
    char username[MAX_USERNAME];
    char log_message[BUFFER_SIZE];

    int was_registered;
    int socket_fd;

    pthread_mutex_lock(&clients_mutex);

    if (!clients[slot].active) {

        pthread_mutex_unlock(&clients_mutex);

        return;
    }

    socket_fd = clients[slot].socket_fd;
    was_registered = clients[slot].registered;

    strncpy(username,
            clients[slot].username,
            sizeof(username) - 1);

    username[sizeof(username) - 1] = '\0';

    clients[slot].active = 0;
    clients[slot].registered = 0;
    clients[slot].socket_fd = -1;
    clients[slot].username[0] = '\0';

    pthread_mutex_unlock(&clients_mutex);

    shutdown(socket_fd, SHUT_RDWR);
    close(socket_fd);

    if (was_registered) {

        snprintf(log_message,
                 sizeof(log_message),
                 "DISCONNECT %s",
                 username);

        log_event(log_message);

        send_leave_notification(slot,
                                username);

    } else {

        log_event(
            "DISCONNECT unregistered_client"
        );
    }
}


/* =========================================================
 * Client handler thread
 * ========================================================= */
static void *client_handler(void *arg)
{
    int slot;
    int socket_fd;

    int registered_seen = 0;

    char line[BUFFER_SIZE];

    slot = *((int *)arg);

    free(arg);

    pthread_mutex_lock(&clients_mutex);

    socket_fd = clients[slot].socket_fd;

    pthread_mutex_unlock(&clients_mutex);


    while (server_running) {

        int result;

        result = recv_line(socket_fd,
                           line,
                           sizeof(line));


        /* Client disconnected. */
        if (result == 0) {
            break;
        }


        /* Receive error. */
        if (result == -1) {

            log_event(
                "CLIENT_RECV_ERROR"
            );

            break;
        }


        /* Line too long. */
        if (result == -2) {

            (void)send_response(
                socket_fd,
                "ERR 007 LINE_TOO_LONG "
                NID_TAG "\n"
            );

            log_event(
                "ERR 007 LINE_TOO_LONG"
            );

            continue;
        }


        /* Remove optional CR from CRLF. */
        line[strcspn(line, "\r")] = '\0';


        /* =================================================
         * FIRST COMMAND MUST BE REGISTER
         * ================================================= */
        if (!registered_seen) {

            const char *prefix = "REGISTER ";


            if (strncmp(line,
                        prefix,
                        strlen(prefix)) != 0) {

                (void)send_response(
                    socket_fd,
                    "ERR 005 NOT_REGISTERED "
                    NID_TAG "\n"
                );

                log_event(
                    "ERR 005 NOT_REGISTERED"
                );

                continue;
            }


            {
                const char *username =
                    line + strlen(prefix);

                int register_result =
                    register_client(
                        slot,
                        username
                    );


                if (register_result == 1) {

                    registered_seen = 1;

                    if (send_response(
                            socket_fd,
                            "OK REGISTERED %s "
                            NID_TAG "\n",
                            username) == -1) {

                        break;
                    }


                    send_join_notification(
                        slot,
                        username
                    );

                } else if (register_result == -1) {

                    (void)send_response(
                        socket_fd,
                        "ERR 001 USERNAME_TAKEN "
                        NID_TAG "\n"
                    );

                    log_event(
                        "ERR 001 USERNAME_TAKEN"
                    );

                } else {

                    (void)send_response(
                        socket_fd,
                        "ERR 006 INVALID_USERNAME "
                        NID_TAG "\n"
                    );

                    log_event(
                        "ERR 006 INVALID_USERNAME"
                    );
                }
            }

            continue;
        }


        /* =================================================
         * BCAST
         * ================================================= */
        if (strncmp(line, "BCAST ", 6) == 0) {

            const char *message =
                line + 6;


            if (message[0] == '\0') {

                (void)send_response(
                    socket_fd,
                    "ERR 007 EMPTY_MESSAGE "
                    NID_TAG "\n"
                );

                log_event(
                    "ERR 007 EMPTY_MESSAGE"
                );

                continue;
            }


            if (strlen(message) >= BUFFER_SIZE / 2) {

                (void)send_response(
                    socket_fd,
                    "ERR 007 MESSAGE_TOO_LONG "
                    NID_TAG "\n"
                );

                log_event(
                    "ERR 007 MESSAGE_TOO_LONG"
                );

                continue;
            }


            handle_broadcast(
                slot,
                message
            );


            (void)send_response(
                socket_fd,
                "OK SENT " NID_TAG "\n"
            );

            continue;
        }


        /* =================================================
         * LIST
         * ================================================= */
        if (strcmp(line, "LIST") == 0) {

            handle_list(socket_fd);

            continue;
        }


        /* =================================================
         * QUIT
         * ================================================= */
        if (strcmp(line, "QUIT") == 0) {

            (void)send_response(
                socket_fd,
                "OK BYE " NID_TAG "\n"
            );

            log_event(
                "QUIT requested"
            );

            break;
        }


        /* =================================================
         * Commands not implemented yet.
         * ================================================= */
        (void)send_response(
            socket_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 INVALID_COMMAND"
        );
    }


    cleanup_client(slot);

    return NULL;
}


/* =========================================================
 * Create TCP server socket
 * ========================================================= */
static int create_server_socket(void)
{
    int fd;
    int reuse_address = 1;

    struct sockaddr_in server_address;


    fd = socket(AF_INET,
                SOCK_STREAM,
                0);

    if (fd == -1) {

        perror("socket");

        return -1;
    }


    if (setsockopt(fd,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   &reuse_address,
                   sizeof(reuse_address)) == -1) {

        perror("setsockopt(SO_REUSEADDR)");

        close(fd);

        return -1;
    }


    memset(&server_address,
           0,
           sizeof(server_address));


    server_address.sin_family =
        AF_INET;

    server_address.sin_addr.s_addr =
        htonl(INADDR_ANY);

    server_address.sin_port =
        htons(SERVER_PORT);


    if (bind(fd,
             (struct sockaddr *)&server_address,
             sizeof(server_address)) == -1) {

        perror("bind");

        close(fd);

        return -1;
    }


    if (listen(fd,
               BACKLOG) == -1) {

        perror("listen");

        close(fd);

        return -1;
    }


    return fd;
}


/* =========================================================
 * Main
 * ========================================================= */
int main(void)
{
    struct sigaction signal_action;


    signal(SIGPIPE, SIG_IGN);


    memset(&signal_action,
           0,
           sizeof(signal_action));

    signal_action.sa_handler =
        handle_signal;

    sigemptyset(&signal_action.sa_mask);


    if (sigaction(SIGINT,
                  &signal_action,
                  NULL) == -1) {

        perror("sigaction");

        return EXIT_FAILURE;
    }


    /* Initialise client table. */
    for (int index = 0;
         index < MAX_CLIENTS;
         index++) {

        clients[index].socket_fd = -1;
        clients[index].active = 0;
        clients[index].registered = 0;
        clients[index].username[0] = '\0';
    }


    listen_fd =
        create_server_socket();


    if (listen_fd == -1) {
        return EXIT_FAILURE;
    }


    log_event(
        "SERVER_STARTED port=6826"
    );


    printf(
        "NetMessenger server started.\n"
    );

    printf(
        "Listening on TCP port: 6826\n"
    );

    printf(
        "Concurrency model: "
        "thread-per-client (pthread)\n"
    );

    printf(
        "Protocol implemented: "
        "REGISTER, LIST, BCAST, QUIT\n"
    );

    printf(
        "Press Ctrl+C to stop the server.\n"
    );


    while (server_running) {

        struct sockaddr_in client_address;

        socklen_t client_length =
            sizeof(client_address);

        int accepted_fd;
        int slot;
        int *slot_arg;

        pthread_t thread_id;


        accepted_fd =
            accept(
                listen_fd,
                (struct sockaddr *)&client_address,
                &client_length
            );


        if (accepted_fd == -1) {

            if (!server_running) {
                break;
            }

            if (errno == EINTR) {
                continue;
            }

            perror("accept");

            continue;
        }


        slot =
            allocate_client_slot(
                accepted_fd
            );


        if (slot == -1) {

            (void)send_response(
                accepted_fd,
                "ERR 008 SERVER_FULL "
                NID_TAG "\n"
            );

            close(accepted_fd);

            log_event(
                "ERR 008 SERVER_FULL"
            );

            continue;
        }


        slot_arg =
            malloc(sizeof(*slot_arg));


        if (slot_arg == NULL) {

            log_event(
                "MEMORY_ALLOCATION_FAILED"
            );

            cleanup_client(slot);

            continue;
        }


        *slot_arg = slot;


        if (pthread_create(
                &thread_id,
                NULL,
                client_handler,
                slot_arg
            ) != 0) {

            log_event(
                "THREAD_CREATE_FAILED"
            );

            free(slot_arg);

            cleanup_client(slot);

            continue;
        }


        pthread_detach(thread_id);
    }


    log_event(
        "SERVER_STOPPED"
    );


    pthread_mutex_lock(&clients_mutex);

    for (int index = 0;
         index < MAX_CLIENTS;
         index++) {

        if (clients[index].active) {

            shutdown(
                clients[index].socket_fd,
                SHUT_RDWR
            );

            close(
                clients[index].socket_fd
            );

            clients[index].active = 0;
            clients[index].registered = 0;
        }
    }

    pthread_mutex_unlock(&clients_mutex);


    if (listen_fd != -1) {

        close(listen_fd);

        listen_fd = -1;
    }


    pthread_mutex_destroy(
        &clients_mutex
    );

    pthread_mutex_destroy(
        &log_mutex
    );


    printf(
        "NetMessenger server stopped.\n"
    );


    return EXIT_SUCCESS;
}
