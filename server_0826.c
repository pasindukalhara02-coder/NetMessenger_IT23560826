/*
 * NetMessenger - Multi-Client Chat and File-Sharing Platform
 *
 * Module       : IE3010 Network Programming
 * Student      : Pasindu Kalhara
 * Registration : IT23560826
 *
 * Personalisation:
 *   Last 4 digits : 0826
 *   Server port   : 6826
 *   NID           : NID:5608
 *
 * Features implemented so far:
 *   - TCP socket server
 *   - Thread-per-client concurrency
 *   - REGISTER
 *   - Unique username checking
 *   - LIST
 *   - Join/leave presence notifications
 *   - BCAST
 *   - PMSG
 *   - QUIT
 *   - TCP line framing
 *   - Personalised NID responses
 *   - Timestamped server logging
 *   - Graceful client cleanup
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
#define LOG_BUFFER_SIZE 2048

#define LOG_FILE "netmsg_IT23560826.log"
#define NID_TAG "NID:5608"

typedef struct
{
    int socket_fd;
    int active;
    int registered;
    char username[MAX_USERNAME];
} ClientInfo;

static volatile sig_atomic_t server_running = 1;
static int listen_fd = -1;

static ClientInfo clients[MAX_CLIENTS];

static pthread_mutex_t clients_mutex =
    PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t log_mutex =
    PTHREAD_MUTEX_INITIALIZER;


/* ---------------------------------------------------------
 * Signal handling
 * --------------------------------------------------------- */

static void handle_signal(int signal_number)
{
    (void)signal_number;

    server_running = 0;

    if (listen_fd != -1)
    {
        shutdown(listen_fd, SHUT_RDWR);
        close(listen_fd);
        listen_fd = -1;
    }
}


/* ---------------------------------------------------------
 * Server logging
 * --------------------------------------------------------- */

static void log_event(const char *event)
{
    FILE *log_file;

    time_t current_time;
    struct tm time_info;

    char timestamp[64];

    current_time = time(NULL);

    if (localtime_r(&current_time, &time_info) == NULL)
    {
        return;
    }

    if (strftime(timestamp,
                 sizeof(timestamp),
                 "%Y-%m-%d %H:%M:%S",
                 &time_info) == 0)
    {
        return;
    }

    pthread_mutex_lock(&log_mutex);

    log_file = fopen(LOG_FILE, "a");

    if (log_file != NULL)
    {
        fprintf(log_file,
                "[%s] %s\n",
                timestamp,
                event);

        fclose(log_file);
    }

    pthread_mutex_unlock(&log_mutex);
}


/* ---------------------------------------------------------
 * Reliable send
 * --------------------------------------------------------- */

static ssize_t send_all(int socket_fd,
                        const void *data,
                        size_t length)
{
    const char *buffer = (const char *)data;

    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent;

        sent = send(socket_fd,
                    buffer + total_sent,
                    length - total_sent,
                    MSG_NOSIGNAL);

        if (sent > 0)
        {
            total_sent += (size_t)sent;
            continue;
        }

        if (sent == -1 && errno == EINTR)
        {
            continue;
        }

        return -1;
    }

    return (ssize_t)total_sent;
}


/* ---------------------------------------------------------
 * Send protocol response
 *
 * Every OK/ERR response contains NID:5608.
 * --------------------------------------------------------- */

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
        (size_t)length >= sizeof(response))
    {
        return -1;
    }

    return (int)send_all(socket_fd,
                          response,
                          (size_t)length);
}


/* ---------------------------------------------------------
 * Receive exactly one line
 *
 * Return:
 *   1  = line received
 *   0  = peer closed
 *  -1  = receive error
 *  -2  = line too long
 * --------------------------------------------------------- */

static int recv_line(int socket_fd,
                     char *buffer,
                     size_t buffer_size)
{
    size_t position = 0;

    if (buffer_size < 2)
    {
        return -1;
    }

    while (1)
    {
        char ch;

        ssize_t received;

        received = recv(socket_fd,
                        &ch,
                        1,
                        0);

        if (received == 1)
        {
            if (ch == '\n')
            {
                buffer[position] = '\0';

                return 1;
            }

            if (position + 1 < buffer_size)
            {
                buffer[position++] = ch;
            }
            else
            {
                do
                {
                    received = recv(socket_fd,
                                    &ch,
                                    1,
                                    0);

                    if (received <= 0)
                    {
                        break;
                    }

                } while (ch != '\n');

                buffer[buffer_size - 1] = '\0';

                return -2;
            }

            continue;
        }

        if (received == 0)
        {
            return 0;
        }

        if (errno == EINTR)
        {
            continue;
        }

        return -1;
    }
}


/* ---------------------------------------------------------
 * Find registered user
 *
 * Caller must hold clients_mutex.
 * --------------------------------------------------------- */

static int find_registered_user(const char *username)
{
    int index;

    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (clients[index].active &&
            clients[index].registered &&
            strcmp(clients[index].username,
                   username) == 0)
        {
            return index;
        }
    }

    return -1;
}


/* ---------------------------------------------------------
 * Allocate client slot
 * --------------------------------------------------------- */

static int allocate_client_slot(int socket_fd)
{
    int index;

    pthread_mutex_lock(&clients_mutex);

    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (!clients[index].active)
        {
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


/* ---------------------------------------------------------
 * Validate username
 * --------------------------------------------------------- */

static int validate_username(const char *username)
{
    size_t index;

    if (username == NULL ||
        username[0] == '\0' ||
        strlen(username) >= MAX_USERNAME)
    {
        return 0;
    }

    for (index = 0;
         username[index] != '\0';
         index++)
    {
        if (isspace((unsigned char)username[index]))
        {
            return 0;
        }
    }

    return 1;
}


/* ---------------------------------------------------------
 * Register client
 *
 * Return:
 *   1  = success
 *   0  = invalid username
 *  -1  = username already exists
 * --------------------------------------------------------- */

static int register_client(int slot,
                           const char *username)
{
    char log_message[BUFFER_SIZE];

    if (!validate_username(username))
    {
        return 0;
    }

    pthread_mutex_lock(&clients_mutex);

    if (clients[slot].registered ||
        find_registered_user(username) != -1)
    {
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


/* ---------------------------------------------------------
 * JOIN notification
 *
 * MSG JOIN <username>
 * --------------------------------------------------------- */

static void send_join_notification(int sender_slot,
                                   const char *username)
{
    char message[BUFFER_SIZE];

    int target_fds[MAX_CLIENTS];

    int target_count = 0;

    int index;

    snprintf(message,
             sizeof(message),
             "MSG JOIN %s\n",
             username);

    pthread_mutex_lock(&clients_mutex);

    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (index != sender_slot &&
            clients[index].active &&
            clients[index].registered)
        {
            target_fds[target_count++] =
                clients[index].socket_fd;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    for (index = 0;
         index < target_count;
         index++)
    {
        (void)send_all(target_fds[index],
                        message,
                        strlen(message));
    }
}


/* ---------------------------------------------------------
 * LEAVE notification
 *
 * MSG LEAVE <username>
 * --------------------------------------------------------- */

static void send_leave_notification(int leaving_slot,
                                    const char *username)
{
    char message[BUFFER_SIZE];

    int target_fds[MAX_CLIENTS];

    int target_count = 0;

    int index;

    snprintf(message,
             sizeof(message),
             "MSG LEAVE %s\n",
             username);

    pthread_mutex_lock(&clients_mutex);

    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (index != leaving_slot &&
            clients[index].active &&
            clients[index].registered)
        {
            target_fds[target_count++] =
                clients[index].socket_fd;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    for (index = 0;
         index < target_count;
         index++)
    {
        (void)send_all(target_fds[index],
                        message,
                        strlen(message));
    }
}


/* ---------------------------------------------------------
 * LIST
 *
 * OK USERS <users> NID:5608
 * --------------------------------------------------------- */

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

    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (clients[index].active &&
            clients[index].registered)
        {
            int written;

            written = snprintf(
                response + used,
                sizeof(response) - used,
                "%s%s",
                first ? "" : ",",
                clients[index].username
            );

            if (written < 0 ||
                (size_t)written >=
                    sizeof(response) - used)
            {
                break;
            }

            used += (size_t)written;

            first = 0;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    if (used +
        strlen(" " NID_TAG "\n") >=
        sizeof(response))
    {
        (void)send_response(
            socket_fd,
            "ERR 007 RESPONSE_TOO_LARGE "
            NID_TAG "\n"
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


/* ---------------------------------------------------------
 * BCAST
 *
 * Command:
 * BCAST <message>
 *
 * Sender:
 * OK SENT NID:5608
 *
 * Other clients:
 * MSG BCAST <sender> <message>
 * --------------------------------------------------------- */

static void handle_broadcast(int sender_slot,
                             int sender_fd,
                             const char *message)
{
    char sender_username[MAX_USERNAME];

    char outgoing[BUFFER_SIZE];

    int target_fds[MAX_CLIENTS];

    int target_count = 0;

    int index;

    if (message == NULL ||
        message[0] == '\0')
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 EMPTY_BCAST"
        );

        return;
    }

    pthread_mutex_lock(&clients_mutex);

    strncpy(sender_username,
            clients[sender_slot].username,
            sizeof(sender_username) - 1);

    sender_username[
        sizeof(sender_username) - 1
    ] = '\0';

    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (index != sender_slot &&
            clients[index].active &&
            clients[index].registered)
        {
            target_fds[target_count++] =
                clients[index].socket_fd;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    if (strlen(message) +
        strlen(sender_username) +
        strlen("MSG BCAST  \n") >=
        sizeof(outgoing))
    {
        (void)send_response(
            sender_fd,
            "ERR 007 MESSAGE_TOO_LONG "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 BCAST_MESSAGE_TOO_LONG"
        );

        return;
    }

    snprintf(outgoing,
             sizeof(outgoing),
             "MSG BCAST %s %s\n",
             sender_username,
             message);

    for (index = 0;
         index < target_count;
         index++)
    {
        (void)send_all(
            target_fds[index],
            outgoing,
            strlen(outgoing)
        );
    }

    (void)send_response(
        sender_fd,
        "OK SENT " NID_TAG "\n"
    );

    {
        char log_message[LOG_BUFFER_SIZE];

        size_t prefix_length;
        size_t message_length;
        size_t copy_length;

        snprintf(log_message,
                 sizeof(log_message),
                 "BCAST from=%s message=",
                 sender_username);

        prefix_length = strlen(log_message);
        message_length = strlen(message);

        if (prefix_length <
            sizeof(log_message) - 1)
        {
            copy_length =
                sizeof(log_message) -
                prefix_length - 1;

            if (message_length <
                copy_length)
            {
                copy_length = message_length;
            }

            memcpy(log_message + prefix_length,
                   message,
                   copy_length);

            log_message[
                prefix_length + copy_length
            ] = '\0';
        }

        log_event(log_message);
    }
}


/* ---------------------------------------------------------
 * PMSG helper
 *
 * Builds:
 * MSG PRIV <sender> <message>\n
 *
 * Uses explicit length checking instead of snprintf("%s")
 * for the entire dynamic line, avoiding truncation warnings.
 * --------------------------------------------------------- */

static int build_private_message(
    char *outgoing,
    size_t outgoing_size,
    const char *sender_username,
    const char *message)
{
    const char prefix[] = "MSG PRIV ";

    size_t prefix_length;
    size_t sender_length;
    size_t message_length;

    size_t required_length;

    size_t position = 0;

    if (outgoing == NULL ||
        sender_username == NULL ||
        message == NULL)
    {
        return -1;
    }

    prefix_length = strlen(prefix);

    sender_length = strlen(sender_username);

    message_length = strlen(message);

    /*
     * Required bytes:
     * prefix
     * sender username
     * one separator space
     * message
     * newline
     * terminating '\0'
     */
    required_length =
        prefix_length +
        sender_length +
        1 +
        message_length +
        1 +
        1;

    if (required_length > outgoing_size)
    {
        return -1;
    }

    memcpy(outgoing + position,
           prefix,
           prefix_length);

    position += prefix_length;

    memcpy(outgoing + position,
           sender_username,
           sender_length);

    position += sender_length;

    outgoing[position++] = ' ';

    memcpy(outgoing + position,
           message,
           message_length);

    position += message_length;

    outgoing[position++] = '\n';

    outgoing[position] = '\0';

    return 0;
}


/* ---------------------------------------------------------
 * PMSG
 *
 * Command:
 * PMSG <username> <message>
 *
 * Sender:
 * OK SENT NID:5608
 *
 * Target:
 * MSG PRIV <sender> <message>
 * --------------------------------------------------------- */

static void handle_private_message(int sender_slot,
                                   int sender_fd,
                                   const char *command)
{
    const char *space_after_target;

    char target_username[MAX_USERNAME];

    char message[BUFFER_SIZE];

    char sender_username[MAX_USERNAME];

    char outgoing[BUFFER_SIZE];

    int target_slot;

    int target_fd = -1;

    size_t target_length;

    size_t message_length;

    if (command == NULL ||
        command[0] == '\0')
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 EMPTY_PMSG"
        );

        return;
    }


    /*
     * Find separator between target
     * and message.
     */
    space_after_target =
        strchr(command, ' ');

    if (space_after_target == NULL)
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 MALFORMED_PMSG"
        );

        return;
    }


    target_length =
        (size_t)(space_after_target - command);

    if (target_length == 0 ||
        target_length >= sizeof(target_username))
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 INVALID_PMSG_TARGET"
        );

        return;
    }


    memcpy(target_username,
           command,
           target_length);

    target_username[target_length] = '\0';


    /*
     * Skip separator spaces.
     */
    while (*space_after_target == ' ')
    {
        space_after_target++;
    }


    if (*space_after_target == '\0')
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 EMPTY_PMSG_MESSAGE"
        );

        return;
    }


    strncpy(message,
            space_after_target,
            sizeof(message) - 1);

    message[sizeof(message) - 1] =
        '\0';


    /*
     * Remove CR if CRLF was used.
     */
    message[strcspn(message, "\r")] =
        '\0';


    message_length = strlen(message);

    if (message_length == 0)
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 EMPTY_PMSG_MESSAGE"
        );

        return;
    }


    /*
     * Get sender username and target socket.
     */
    pthread_mutex_lock(&clients_mutex);

    strncpy(sender_username,
            clients[sender_slot].username,
            sizeof(sender_username) - 1);

    sender_username[
        sizeof(sender_username) - 1
    ] = '\0';

    target_slot =
        find_registered_user(target_username);

    if (target_slot != -1)
    {
        target_fd =
            clients[target_slot].socket_fd;
    }

    pthread_mutex_unlock(&clients_mutex);


    /*
     * Target does not exist.
     */
    if (target_slot == -1)
    {
        (void)send_response(
            sender_fd,
            "ERR 002 USER_NOT_FOUND "
            NID_TAG "\n"
        );

        {
            char log_message[LOG_BUFFER_SIZE];

            snprintf(
                log_message,
                sizeof(log_message),
                "ERR 002 USER_NOT_FOUND "
                "target=%s sender=%s",
                target_username,
                sender_username
            );

            log_event(log_message);
        }

        return;
    }


    /*
     * Build exact protocol forwarding line.
     */
    if (build_private_message(
            outgoing,
            sizeof(outgoing),
            sender_username,
            message) == -1)
    {
        (void)send_response(
            sender_fd,
            "ERR 007 MESSAGE_TOO_LONG "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 PMSG_MESSAGE_TOO_LONG"
        );

        return;
    }


    /*
     * Send to target only.
     */
    if (send_all(
            target_fd,
            outgoing,
            strlen(outgoing)) == -1)
    {
        log_event(
            "PMSG target send failed"
        );
    }


    /*
     * Required sender response.
     */
    (void)send_response(
        sender_fd,
        "OK SENT " NID_TAG "\n"
    );


    /*
     * Timestamped PMSG logging.
     */
    {
        char log_message[LOG_BUFFER_SIZE];

        size_t used;
        size_t remaining;
        size_t copy_length;

        snprintf(
            log_message,
            sizeof(log_message),
            "PMSG from=%s to=%s message=",
            sender_username,
            target_username
        );

        used = strlen(log_message);

        if (used < sizeof(log_message) - 1)
        {
            remaining =
                sizeof(log_message) -
                used - 1;

            copy_length = message_length;

            if (copy_length > remaining)
            {
                copy_length = remaining;
            }

            memcpy(
                log_message + used,
                message,
                copy_length
            );

            log_message[
                used + copy_length
            ] = '\0';
        }

        log_event(log_message);
    }
}


/* ---------------------------------------------------------
 * Cleanup client
 * --------------------------------------------------------- */

static void cleanup_client(int slot)
{
    char username[MAX_USERNAME];

    char log_message[BUFFER_SIZE];

    int was_registered;

    int socket_fd;

    pthread_mutex_lock(&clients_mutex);

    if (!clients[slot].active)
    {
        pthread_mutex_unlock(&clients_mutex);

        return;
    }

    socket_fd =
        clients[slot].socket_fd;

    was_registered =
        clients[slot].registered;

    strncpy(username,
            clients[slot].username,
            sizeof(username) - 1);

    username[sizeof(username) - 1] =
        '\0';

    clients[slot].active = 0;

    clients[slot].registered = 0;

    clients[slot].socket_fd = -1;

    clients[slot].username[0] = '\0';

    pthread_mutex_unlock(&clients_mutex);


    shutdown(socket_fd, SHUT_RDWR);

    close(socket_fd);


    if (was_registered)
    {
        snprintf(
            log_message,
            sizeof(log_message),
            "DISCONNECT %s",
            username
        );

        log_event(log_message);

        send_leave_notification(
            slot,
            username
        );
    }
    else
    {
        log_event(
            "DISCONNECT unregistered_client"
        );
    }
}


/* ---------------------------------------------------------
 * Client thread
 * --------------------------------------------------------- */

static void *client_handler(void *arg)
{
    int slot;

    int socket_fd;

    int registered_seen = 0;

    char line[BUFFER_SIZE];


    slot = *((int *)arg);

    free(arg);


    pthread_mutex_lock(&clients_mutex);

    socket_fd =
        clients[slot].socket_fd;

    pthread_mutex_unlock(&clients_mutex);


    while (server_running)
    {
        int result;


        result = recv_line(
            socket_fd,
            line,
            sizeof(line)
        );


        /*
         * Unexpected disconnect.
         */
        if (result == 0)
        {
            log_event(
                "CLIENT_DISCONNECTED_UNEXPECTED"
            );

            break;
        }


        /*
         * Receive error.
         */
        if (result == -1)
        {
            log_event(
                "CLIENT_RECV_ERROR"
            );

            break;
        }


        /*
         * Oversized command.
         */
        if (result == -2)
        {
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


        /*
         * Support CRLF.
         */
        line[strcspn(line, "\r")] =
            '\0';


        /* -------------------------------------------------
         * REGISTER must be first
         * ------------------------------------------------- */

        if (!registered_seen)
        {
            const char *prefix =
                "REGISTER ";

            if (strncmp(
                    line,
                    prefix,
                    strlen(prefix)) != 0)
            {
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

                int register_result;


                register_result =
                    register_client(
                        slot,
                        username
                    );


                if (register_result == 1)
                {
                    registered_seen = 1;


                    if (send_response(
                            socket_fd,
                            "OK REGISTERED %s "
                            NID_TAG "\n",
                            username) == -1)
                    {
                        break;
                    }


                    send_join_notification(
                        slot,
                        username
                    );
                }
                else if (register_result == -1)
                {
                    (void)send_response(
                        socket_fd,
                        "ERR 001 USERNAME_TAKEN "
                        NID_TAG "\n"
                    );

                    log_event(
                        "ERR 001 USERNAME_TAKEN"
                    );
                }
                else
                {
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


        /* -------------------------------------------------
         * LIST
         * ------------------------------------------------- */

        if (strcmp(line, "LIST") == 0)
        {
            handle_list(socket_fd);

            continue;
        }


        /* -------------------------------------------------
         * BCAST
         * ------------------------------------------------- */

        if (strncmp(line,
                    "BCAST ",
                    6) == 0)
        {
            handle_broadcast(
                slot,
                socket_fd,
                line + 6
            );

            continue;
        }


        /* -------------------------------------------------
         * PMSG
         *
         * Expected:
         * PMSG <username> <message>
         * ------------------------------------------------- */

        if (strncmp(line,
                    "PMSG ",
                    5) == 0)
        {
            handle_private_message(
                slot,
                socket_fd,
                line + 5
            );

            continue;
        }


        /* -------------------------------------------------
         * QUIT
         * ------------------------------------------------- */

        if (strcmp(line, "QUIT") == 0)
        {
            (void)send_response(
                socket_fd,
                "OK BYE " NID_TAG "\n"
            );

            log_event(
                "QUIT requested"
            );

            break;
        }


        /* -------------------------------------------------
         * Invalid command
         * ------------------------------------------------- */

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


/* ---------------------------------------------------------
 * Create TCP server socket
 * --------------------------------------------------------- */

static int create_server_socket(void)
{
    int fd;

    int reuse_address = 1;

    struct sockaddr_in server_address;


    fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );


    if (fd == -1)
    {
        perror("socket");

        return -1;
    }


    if (setsockopt(
            fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse_address,
            sizeof(reuse_address)) == -1)
    {
        perror(
            "setsockopt(SO_REUSEADDR)"
        );

        close(fd);

        return -1;
    }


    memset(
        &server_address,
        0,
        sizeof(server_address)
    );


    server_address.sin_family =
        AF_INET;


    server_address.sin_addr.s_addr =
        htonl(INADDR_ANY);


    server_address.sin_port =
        htons(SERVER_PORT);


    if (bind(
            fd,
            (struct sockaddr *)&server_address,
            sizeof(server_address)) == -1)
    {
        perror("bind");

        close(fd);

        return -1;
    }


    if (listen(fd, BACKLOG) == -1)
    {
        perror("listen");

        close(fd);

        return -1;
    }


    return fd;
}


/* ---------------------------------------------------------
 * Main
 * --------------------------------------------------------- */

int main(void)
{
    struct sigaction signal_action;


    /*
     * Prevent SIGPIPE from terminating server.
     */
    signal(SIGPIPE, SIG_IGN);


    memset(
        &signal_action,
        0,
        sizeof(signal_action)
    );


    signal_action.sa_handler =
        handle_signal;


    sigemptyset(
        &signal_action.sa_mask
    );


    if (sigaction(
            SIGINT,
            &signal_action,
            NULL) == -1)
    {
        perror("sigaction");

        return EXIT_FAILURE;
    }


    /*
     * Initialise client table.
     */
    for (int index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        clients[index].socket_fd = -1;

        clients[index].active = 0;

        clients[index].registered = 0;

        clients[index].username[0] = '\0';
    }


    /*
     * Create listening socket.
     */
    listen_fd =
        create_server_socket();


    if (listen_fd == -1)
    {
        return EXIT_FAILURE;
    }


    /*
     * Server startup log.
     */
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
        "REGISTER, LIST, BCAST, PMSG, QUIT\n"
    );

    printf(
        "NID: NID:5608\n"
    );

    printf(
        "Press Ctrl+C to stop the server.\n"
    );


    /*
     * Accept clients.
     */
    while (server_running)
    {
        struct sockaddr_in client_address;

        socklen_t client_length =
            sizeof(client_address);

        int accepted_fd;

        int slot;

        int *slot_arg;

        pthread_t thread_id;


        accepted_fd = accept(
            listen_fd,
            (struct sockaddr *)&client_address,
            &client_length
        );


        if (accepted_fd == -1)
        {
            if (!server_running)
            {
                break;
            }

            if (errno == EINTR)
            {
                continue;
            }

            perror("accept");

            continue;
        }


        /*
         * Allocate client slot.
         */
        slot =
            allocate_client_slot(
                accepted_fd
            );


        if (slot == -1)
        {
            (void)send_response(
                accepted_fd,
                "ERR 008 SERVER_FULL "
                NID_TAG "\n"
            );

            log_event(
                "ERR 008 SERVER_FULL"
            );

            close(accepted_fd);

            continue;
        }


        /*
         * Allocate thread argument.
         */
        slot_arg =
            malloc(sizeof(*slot_arg));


        if (slot_arg == NULL)
        {
            log_event(
                "MEMORY_ALLOCATION_FAILED"
            );

            cleanup_client(slot);

            continue;
        }


        *slot_arg = slot;


        /*
         * Create detached client thread.
         */
        if (pthread_create(
                &thread_id,
                NULL,
                client_handler,
                slot_arg) != 0)
        {
            log_event(
                "THREAD_CREATE_FAILED"
            );

            free(slot_arg);

            cleanup_client(slot);

            continue;
        }


        pthread_detach(thread_id);
    }


    /*
     * Server shutdown.
     */
    log_event(
        "SERVER_STOPPED"
    );


    pthread_mutex_lock(
        &clients_mutex
    );


    for (int index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (clients[index].active)
        {
            shutdown(
                clients[index].socket_fd,
                SHUT_RDWR
            );

            close(
                clients[index].socket_fd
            );

            clients[index].active = 0;

            clients[index].registered = 0;

            clients[index].socket_fd = -1;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    if (listen_fd != -1)
    {
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
