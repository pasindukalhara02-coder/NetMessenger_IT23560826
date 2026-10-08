/*
 * NetMessenger - Multi-Client Chat and File-Sharing Platform
 * IE3010 Network Programming
 *
 * Student       : Pasindu Kalhara
 * Registration  : IT23560826
 *
 * Personalisation:
 *   Last 4 digits : 0826
 *   Server port   : 6826
 *   NID           : NID:5608
 *
 * Implemented:
 *   REGISTER
 *   LIST
 *   BCAST
 *   PMSG
 *   JOIN
 *   LEAVE
 *   ROOMS
 *   RMSG
 *   QUIT
 *
 * Also includes:
 *   - Thread-per-client concurrency
 *   - TCP line framing
 *   - Unique usernames
 *   - Presence notifications
 *   - Room management
 *   - Timestamped server logging
 *   - Graceful client cleanup
 *   - Personalised OK/ERR responses
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
#define LOG_BUFFER_SIZE 2048

#define MAX_CLIENTS 100
#define MAX_USERNAME 64

#define MAX_ROOMS 50
#define MAX_ROOM_NAME 64

#define LOG_FILE "netmsg_IT23560826.log"
#define NID_TAG "NID:5608"


/* =========================================================
 * Client structure
 * ========================================================= */

typedef struct
{
    int socket_fd;
    int active;
    int registered;

    char username[MAX_USERNAME];

} ClientInfo;


/* =========================================================
 * Room structure
 * ========================================================= */

typedef struct
{
    int active;

    char name[MAX_ROOM_NAME];

    int members[MAX_CLIENTS];

} RoomInfo;


/* =========================================================
 * Global state
 * ========================================================= */

static volatile sig_atomic_t server_running = 1;

static int listen_fd = -1;

static ClientInfo clients[MAX_CLIENTS];

static RoomInfo rooms[MAX_ROOMS];

static pthread_mutex_t clients_mutex =
    PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t log_mutex =
    PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
 * Signal handling
 * ========================================================= */

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


/* =========================================================
 * Server logging
 * ========================================================= */

static void log_event(const char *event)
{
    FILE *log_file;

    time_t current_time;

    struct tm time_info;

    char timestamp[64];


    current_time = time(NULL);


    if (localtime_r(
            &current_time,
            &time_info) == NULL)
    {
        return;
    }


    if (strftime(
            timestamp,
            sizeof(timestamp),
            "%Y-%m-%d %H:%M:%S",
            &time_info) == 0)
    {
        return;
    }


    pthread_mutex_lock(&log_mutex);


    log_file = fopen(
        LOG_FILE,
        "a"
    );


    if (log_file != NULL)
    {
        fprintf(
            log_file,
            "[%s] %s\n",
            timestamp,
            event
        );

        fclose(log_file);
    }


    pthread_mutex_unlock(&log_mutex);
}


/* =========================================================
 * Reliable send
 * ========================================================= */

static ssize_t send_all(
    int socket_fd,
    const void *data,
    size_t length)
{
    const char *buffer =
        (const char *)data;

    size_t total_sent = 0;


    while (total_sent < length)
    {
        ssize_t sent;


        sent = send(
            socket_fd,
            buffer + total_sent,
            length - total_sent,
            MSG_NOSIGNAL
        );


        if (sent > 0)
        {
            total_sent +=
                (size_t)sent;

            continue;
        }


        if (sent == -1 &&
            errno == EINTR)
        {
            continue;
        }


        return -1;
    }


    return (ssize_t)total_sent;
}


/* =========================================================
 * Protocol response
 *
 * Every OK/ERR response includes NID:5608.
 * ========================================================= */

static int send_response(
    int socket_fd,
    const char *format,
    ...)
{
    char response[BUFFER_SIZE];

    va_list args;

    int length;


    va_start(args, format);


    length = vsnprintf(
        response,
        sizeof(response),
        format,
        args
    );


    va_end(args);


    if (length < 0 ||
        (size_t)length >=
            sizeof(response))
    {
        return -1;
    }


    return (int)send_all(
        socket_fd,
        response,
        (size_t)length
    );
}


/* =========================================================
 * Receive one text line
 *
 * Return:
 *   1  = success
 *   0  = connection closed
 *  -1  = receive error
 *  -2  = line too long
 * ========================================================= */

static int recv_line(
    int socket_fd,
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


        received = recv(
            socket_fd,
            &ch,
            1,
            0
        );


        if (received == 1)
        {
            if (ch == '\n')
            {
                buffer[position] =
                    '\0';

                return 1;
            }


            if (position + 1 <
                buffer_size)
            {
                buffer[position++] =
                    ch;
            }
            else
            {
                /*
                 * Drain oversized
                 * line until newline.
                 */

                do
                {
                    received = recv(
                        socket_fd,
                        &ch,
                        1,
                        0
                    );


                    if (received <= 0)
                    {
                        break;
                    }

                } while (ch != '\n');


                buffer[
                    buffer_size - 1
                ] = '\0';


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


/* =========================================================
 * Find registered user
 *
 * Caller must hold clients_mutex.
 * ========================================================= */

static int find_registered_user(
    const char *username)
{
    int index;


    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (clients[index].active &&
            clients[index].registered &&
            strcmp(
                clients[index].username,
                username
            ) == 0)
        {
            return index;
        }
    }


    return -1;
}


/* =========================================================
 * Find room
 *
 * Caller must hold clients_mutex.
 * ========================================================= */

static int find_room(
    const char *room_name)
{
    int index;


    for (index = 0;
         index < MAX_ROOMS;
         index++)
    {
        if (rooms[index].active &&
            strcmp(
                rooms[index].name,
                room_name
            ) == 0)
        {
            return index;
        }
    }


    return -1;
}


/* =========================================================
 * Create room
 *
 * Caller must hold clients_mutex.
 * ========================================================= */

static int create_room(
    const char *room_name)
{
    int index;


    for (index = 0;
         index < MAX_ROOMS;
         index++)
    {
        if (!rooms[index].active)
        {
            rooms[index].active = 1;


            strncpy(
                rooms[index].name,
                room_name,
                sizeof(rooms[index].name) - 1
            );


            rooms[index].name[
                sizeof(rooms[index].name) - 1
            ] = '\0';


            memset(
                rooms[index].members,
                0,
                sizeof(rooms[index].members)
            );


            return index;
        }
    }


    return -1;
}


/* =========================================================
 * Validate token
 *
 * Used for usernames and room names.
 * ========================================================= */

static int validate_token(
    const char *value,
    size_t max_length)
{
    size_t index;


    if (value == NULL ||
        value[0] == '\0' ||
        strlen(value) >= max_length)
    {
        return 0;
    }


    for (index = 0;
         value[index] != '\0';
         index++)
    {
        if (isspace(
                (unsigned char)value[index]))
        {
            return 0;
        }
    }


    return 1;
}


/* =========================================================
 * Allocate client slot
 * ========================================================= */

static int allocate_client_slot(
    int socket_fd)
{
    int index;


    pthread_mutex_lock(
        &clients_mutex
    );


    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (!clients[index].active)
        {
            clients[index].socket_fd =
                socket_fd;

            clients[index].active =
                1;

            clients[index].registered =
                0;

            clients[index].username[0] =
                '\0';


            pthread_mutex_unlock(
                &clients_mutex
            );


            return index;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    return -1;
}


/* =========================================================
 * Register client
 * ========================================================= */

static int register_client(
    int slot,
    const char *username)
{
    char log_message[
        BUFFER_SIZE
    ];


    if (!validate_token(
            username,
            MAX_USERNAME))
    {
        return 0;
    }


    pthread_mutex_lock(
        &clients_mutex
    );


    if (clients[slot].registered ||
        find_registered_user(
            username) != -1)
    {
        pthread_mutex_unlock(
            &clients_mutex
        );

        return -1;
    }


    strncpy(
        clients[slot].username,
        username,
        sizeof(clients[slot].username) - 1
    );


    clients[slot].username[
        sizeof(clients[slot].username) - 1
    ] = '\0';


    clients[slot].registered = 1;


    snprintf(
        log_message,
        sizeof(log_message),
        "REGISTER %s",
        clients[slot].username
    );


    pthread_mutex_unlock(
        &clients_mutex
    );


    log_event(
        log_message
    );


    return 1;
}


/* =========================================================
 * Join notification
 *
 * MSG JOIN <username>
 * ========================================================= */

static void send_join_notification(
    int sender_slot,
    const char *username)
{
    char message[BUFFER_SIZE];

    int target_fds[MAX_CLIENTS];

    int target_count = 0;

    int index;


    snprintf(
        message,
        sizeof(message),
        "MSG JOIN %s\n",
        username
    );


    pthread_mutex_lock(
        &clients_mutex
    );


    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (index != sender_slot &&
            clients[index].active &&
            clients[index].registered)
        {
            target_fds[
                target_count++
            ] =
                clients[index].socket_fd;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    for (index = 0;
         index < target_count;
         index++)
    {
        (void)send_all(
            target_fds[index],
            message,
            strlen(message)
        );
    }
}


/* =========================================================
 * Leave notification
 *
 * MSG LEAVE <username>
 * ========================================================= */

static void send_leave_notification(
    int leaving_slot,
    const char *username)
{
    char message[BUFFER_SIZE];

    int target_fds[MAX_CLIENTS];

    int target_count = 0;

    int index;


    snprintf(
        message,
        sizeof(message),
        "MSG LEAVE %s\n",
        username
    );


    pthread_mutex_lock(
        &clients_mutex
    );


    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (index != leaving_slot &&
            clients[index].active &&
            clients[index].registered)
        {
            target_fds[
                target_count++
            ] =
                clients[index].socket_fd;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    for (index = 0;
         index < target_count;
         index++)
    {
        (void)send_all(
            target_fds[index],
            message,
            strlen(message)
        );
    }
}


/* =========================================================
 * LIST users
 * ========================================================= */

static void handle_list(
    int socket_fd)
{
    char response[BUFFER_SIZE];

    size_t used;

    int first = 1;

    int index;


    used = (size_t)snprintf(
        response,
        sizeof(response),
        "OK USERS "
    );


    pthread_mutex_lock(
        &clients_mutex
    );


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


            used +=
                (size_t)written;


            first = 0;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


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


    snprintf(
        response + used,
        sizeof(response) - used,
        " " NID_TAG "\n"
    );


    (void)send_all(
        socket_fd,
        response,
        strlen(response)
    );


    log_event(
        "LIST requested"
    );
}


/* =========================================================
 * BCAST
 * ========================================================= */

static void handle_broadcast(
    int sender_slot,
    int sender_fd,
    const char *message)
{
    char sender_username[
        MAX_USERNAME
    ];

    char outgoing[
        BUFFER_SIZE
    ];

    int target_fds[
        MAX_CLIENTS
    ];

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


    pthread_mutex_lock(
        &clients_mutex
    );


    strncpy(
        sender_username,
        clients[sender_slot].username,
        sizeof(sender_username) - 1
    );


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
            target_fds[
                target_count++
            ] =
                clients[index].socket_fd;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    if (strlen("MSG BCAST  \n") +
        strlen(sender_username) +
        strlen(message) >=
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


    snprintf(
        outgoing,
        sizeof(outgoing),
        "MSG BCAST %s %s\n",
        sender_username,
        message
    );


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
        char log_message[
            LOG_BUFFER_SIZE
        ];

        size_t used;
        size_t remaining;
        size_t copy_length;


        snprintf(
            log_message,
            sizeof(log_message),
            "BCAST from=%s message=",
            sender_username
        );


        used = strlen(
            log_message
        );


        remaining =
            sizeof(log_message)
            - used
            - 1;


        copy_length =
            strlen(message);


        if (copy_length > remaining)
        {
            copy_length =
                remaining;
        }


        memcpy(
            log_message + used,
            message,
            copy_length
        );


        log_message[
            used + copy_length
        ] = '\0';


        log_event(
            log_message
        );
    }
}


/* =========================================================
 * Build private message
 * ========================================================= */

static int build_private_message(
    char *outgoing,
    size_t outgoing_size,
    const char *sender_username,
    const char *message)
{
    const char prefix[] =
        "MSG PRIV ";


    size_t prefix_length =
        strlen(prefix);


    size_t sender_length =
        strlen(sender_username);


    size_t message_length =
        strlen(message);


    size_t required =
        prefix_length +
        sender_length +
        1 +
        message_length +
        1 +
        1;


    size_t position = 0;


    if (required > outgoing_size)
    {
        return -1;
    }


    memcpy(
        outgoing + position,
        prefix,
        prefix_length
    );


    position +=
        prefix_length;


    memcpy(
        outgoing + position,
        sender_username,
        sender_length
    );


    position +=
        sender_length;


    outgoing[position++] =
        ' ';


    memcpy(
        outgoing + position,
        message,
        message_length
    );


    position +=
        message_length;


    outgoing[position++] =
        '\n';


    outgoing[position] =
        '\0';


    return 0;
}


/* =========================================================
 * PMSG
 * ========================================================= */

static void handle_private_message(
    int sender_slot,
    int sender_fd,
    const char *command)
{
    const char *separator;

    char target_username[
        MAX_USERNAME
    ];

    char message[
        BUFFER_SIZE
    ];

    char sender_username[
        MAX_USERNAME
    ];

    char outgoing[
        BUFFER_SIZE
    ];

    int target_slot;

    int target_fd = -1;

    size_t target_length;


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


    separator =
        strchr(command, ' ');


    if (separator == NULL)
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
        (size_t)(
            separator - command
        );


    if (target_length == 0 ||
        target_length >=
            sizeof(target_username))
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


    memcpy(
        target_username,
        command,
        target_length
    );


    target_username[
        target_length
    ] = '\0';


    while (*separator == ' ')
    {
        separator++;
    }


    if (*separator == '\0')
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


    strncpy(
        message,
        separator,
        sizeof(message) - 1
    );


    message[
        sizeof(message) - 1
    ] = '\0';


    message[
        strcspn(message, "\r")
    ] = '\0';


    if (message[0] == '\0')
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


    pthread_mutex_lock(
        &clients_mutex
    );


    strncpy(
        sender_username,
        clients[sender_slot].username,
        sizeof(sender_username) - 1
    );


    sender_username[
        sizeof(sender_username) - 1
    ] = '\0';


    target_slot =
        find_registered_user(
            target_username
        );


    if (target_slot != -1)
    {
        target_fd =
            clients[target_slot].socket_fd;
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    if (target_slot == -1)
    {
        (void)send_response(
            sender_fd,
            "ERR 002 USER_NOT_FOUND "
            NID_TAG "\n"
        );


        {
            char log_message[
                LOG_BUFFER_SIZE
            ];


            snprintf(
                log_message,
                sizeof(log_message),
                "ERR 002 USER_NOT_FOUND "
                "target=%s sender=%s",
                target_username,
                sender_username
            );


            log_event(
                log_message
            );
        }


        return;
    }


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


    if (send_all(
            target_fd,
            outgoing,
            strlen(outgoing)) == -1)
    {
        log_event(
            "PMSG target send failed"
        );
    }


    (void)send_response(
        sender_fd,
        "OK SENT " NID_TAG "\n"
    );


    {
        char log_message[
            LOG_BUFFER_SIZE
        ];


        snprintf(
            log_message,
            sizeof(log_message),
            "PMSG from=%s to=%s message=",
            sender_username,
            target_username
        );


        {
            size_t used =
                strlen(log_message);

            size_t remaining =
                sizeof(log_message)
                - used
                - 1;

            size_t copy_length =
                strlen(message);


            if (copy_length >
                remaining)
            {
                copy_length =
                    remaining;
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


        log_event(
            log_message
        );
    }
}


/* =========================================================
 * JOIN room
 *
 * JOIN <room>
 * ========================================================= */

static void handle_join(
    int slot,
    int socket_fd,
    const char *room_name)
{
    int room_slot;


    if (!validate_token(
            room_name,
            MAX_ROOM_NAME))
    {
        (void)send_response(
            socket_fd,
            "ERR 007 INVALID_ROOM "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 INVALID_ROOM"
        );

        return;
    }


    pthread_mutex_lock(
        &clients_mutex
    );


    room_slot =
        find_room(room_name);


    /*
     * Assignment requirement:
     * JOIN creates the room if
     * it does not exist.
     */
    if (room_slot == -1)
    {
        room_slot =
            create_room(room_name);
    }


    if (room_slot == -1)
    {
        pthread_mutex_unlock(
            &clients_mutex
        );


        (void)send_response(
            socket_fd,
            "ERR 007 ROOM_LIMIT "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 ROOM_LIMIT"
        );


        return;
    }


    rooms[room_slot].members[
        slot
    ] = 1;


    pthread_mutex_unlock(
        &clients_mutex
    );


    (void)send_response(
        socket_fd,
        "OK JOINED %s "
        NID_TAG "\n",
        room_name
    );


    {
        char username[
            MAX_USERNAME
        ];

        char log_message[
            LOG_BUFFER_SIZE
        ];


        pthread_mutex_lock(
            &clients_mutex
        );


        strncpy(
            username,
            clients[slot].username,
            sizeof(username) - 1
        );


        username[
            sizeof(username) - 1
        ] = '\0';


        pthread_mutex_unlock(
            &clients_mutex
        );


        snprintf(
            log_message,
            sizeof(log_message),
            "JOIN user=%s room=%s",
            username,
            room_name
        );


        log_event(
            log_message
        );
    }
}


/* =========================================================
 * LEAVE room
 *
 * LEAVE <room>
 * ========================================================= */

static void handle_leave(
    int slot,
    int socket_fd,
    const char *room_name)
{
    int room_slot;

    int was_member;

    int room_empty = 1;

    int index;


    if (!validate_token(
            room_name,
            MAX_ROOM_NAME))
    {
        (void)send_response(
            socket_fd,
            "ERR 007 INVALID_ROOM "
            NID_TAG "\n"
        );

        log_event(
            "ERR 007 INVALID_ROOM"
        );

        return;
    }


    pthread_mutex_lock(
        &clients_mutex
    );


    room_slot =
        find_room(room_name);


    if (room_slot == -1)
    {
        pthread_mutex_unlock(
            &clients_mutex
        );


        (void)send_response(
            socket_fd,
            "ERR 003 ROOM_NOT_FOUND "
            NID_TAG "\n"
        );


        log_event(
            "ERR 003 ROOM_NOT_FOUND"
        );


        return;
    }


    was_member =
        rooms[room_slot].members[
            slot
        ];


    rooms[room_slot].members[
        slot
    ] = 0;


    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (rooms[room_slot].members[index] &&
            clients[index].active &&
            clients[index].registered)
        {
            room_empty = 0;
            break;
        }
    }


    /*
     * Remove empty rooms.
     */
    if (room_empty)
    {
        rooms[room_slot].active = 0;

        rooms[room_slot].name[0] =
            '\0';

        memset(
            rooms[room_slot].members,
            0,
            sizeof(rooms[room_slot].members)
        );
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    if (!was_member)
    {
        (void)send_response(
            socket_fd,
            "ERR 007 NOT_ROOM_MEMBER "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 NOT_ROOM_MEMBER"
        );


        return;
    }


    (void)send_response(
        socket_fd,
        "OK LEFT %s "
        NID_TAG "\n",
        room_name
    );


    {
        char username[
            MAX_USERNAME
        ];

        char log_message[
            LOG_BUFFER_SIZE
        ];


        pthread_mutex_lock(
            &clients_mutex
        );


        strncpy(
            username,
            clients[slot].username,
            sizeof(username) - 1
        );


        username[
            sizeof(username) - 1
        ] = '\0';


        pthread_mutex_unlock(
            &clients_mutex
        );


        snprintf(
            log_message,
            sizeof(log_message),
            "LEAVE user=%s room=%s",
            username,
            room_name
        );


        log_event(
            log_message
        );
    }
}


/* =========================================================
 * ROOMS
 *
 * OK ROOMS <comma-separated-rooms> NID:5608
 * ========================================================= */

static void handle_rooms(
    int socket_fd)
{
    char response[
        BUFFER_SIZE
    ];

    size_t used;

    int first = 1;

    int index;


    used = (size_t)snprintf(
        response,
        sizeof(response),
        "OK ROOMS "
    );


    pthread_mutex_lock(
        &clients_mutex
    );


    for (index = 0;
         index < MAX_ROOMS;
         index++)
    {
        if (rooms[index].active)
        {
            int written;


            written = snprintf(
                response + used,
                sizeof(response) - used,
                "%s%s",
                first ? "" : ",",
                rooms[index].name
            );


            if (written < 0 ||
                (size_t)written >=
                    sizeof(response) - used)
            {
                break;
            }


            used +=
                (size_t)written;


            first = 0;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


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


    snprintf(
        response + used,
        sizeof(response) - used,
        " " NID_TAG "\n"
    );


    (void)send_all(
        socket_fd,
        response,
        strlen(response)
    );


    log_event(
        "ROOMS requested"
    );
}


/* =========================================================
 * Build room message
 *
 * MSG ROOM <room> <sender> <message>
 * ========================================================= */

static int build_room_message(
    char *outgoing,
    size_t outgoing_size,
    const char *room_name,
    const char *sender_username,
    const char *message)
{
    const char prefix[] =
        "MSG ROOM ";


    size_t prefix_length =
        strlen(prefix);


    size_t room_length =
        strlen(room_name);


    size_t sender_length =
        strlen(sender_username);


    size_t message_length =
        strlen(message);


    size_t required =
        prefix_length +
        room_length +
        1 +
        sender_length +
        1 +
        message_length +
        1 +
        1;


    size_t position = 0;


    if (required > outgoing_size)
    {
        return -1;
    }


    memcpy(
        outgoing + position,
        prefix,
        prefix_length
    );


    position +=
        prefix_length;


    memcpy(
        outgoing + position,
        room_name,
        room_length
    );


    position +=
        room_length;


    outgoing[position++] =
        ' ';


    memcpy(
        outgoing + position,
        sender_username,
        sender_length
    );


    position +=
        sender_length;


    outgoing[position++] =
        ' ';


    memcpy(
        outgoing + position,
        message,
        message_length
    );


    position +=
        message_length;


    outgoing[position++] =
        '\n';


    outgoing[position] =
        '\0';


    return 0;
}


/* =========================================================
 * RMSG
 *
 * RMSG <room> <message>
 *
 * Room members receive:
 * MSG ROOM <room> <sender> <message>
 * ========================================================= */

static void handle_room_message(
    int sender_slot,
    int sender_fd,
    const char *command)
{
    const char *separator;


    char room_name[
        MAX_ROOM_NAME
    ];


    char message[
        BUFFER_SIZE
    ];


    char sender_username[
        MAX_USERNAME
    ];


    char outgoing[
        BUFFER_SIZE
    ];


    int target_fds[
        MAX_CLIENTS
    ];


    int target_count = 0;


    int room_slot;


    size_t room_length;


    int index;


    /*
     * Find room/message separator.
     */
    separator =
        strchr(command, ' ');


    if (separator == NULL)
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 MALFORMED_RMSG"
        );


        return;
    }


    room_length =
        (size_t)(
            separator - command
        );


    if (room_length == 0 ||
        room_length >=
            sizeof(room_name))
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 INVALID_RMSG_ROOM"
        );


        return;
    }


    memcpy(
        room_name,
        command,
        room_length
    );


    room_name[
        room_length
    ] = '\0';


    /*
     * Skip spaces after room name.
     */
    while (*separator == ' ')
    {
        separator++;
    }


    if (*separator == '\0')
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 EMPTY_RMSG_MESSAGE"
        );


        return;
    }


    strncpy(
        message,
        separator,
        sizeof(message) - 1
    );


    message[
        sizeof(message) - 1
    ] = '\0';


    message[
        strcspn(message, "\r")
    ] = '\0';


    if (message[0] == '\0')
    {
        (void)send_response(
            sender_fd,
            "ERR 007 INVALID_COMMAND "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 EMPTY_RMSG_MESSAGE"
        );


        return;
    }


    pthread_mutex_lock(
        &clients_mutex
    );


    room_slot =
        find_room(room_name);


    if (room_slot == -1)
    {
        pthread_mutex_unlock(
            &clients_mutex
        );


        (void)send_response(
            sender_fd,
            "ERR 003 ROOM_NOT_FOUND "
            NID_TAG "\n"
        );


        log_event(
            "ERR 003 ROOM_NOT_FOUND"
        );


        return;
    }


    /*
     * Sender must be a room member.
     */
    if (!rooms[room_slot].members[
            sender_slot])
    {
        pthread_mutex_unlock(
            &clients_mutex
        );


        (void)send_response(
            sender_fd,
            "ERR 007 NOT_ROOM_MEMBER "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 NOT_ROOM_MEMBER"
        );


        return;
    }


    strncpy(
        sender_username,
        clients[sender_slot].username,
        sizeof(sender_username) - 1
    );


    sender_username[
        sizeof(sender_username) - 1
    ] = '\0';


    /*
     * Build exact MSG ROOM line.
     */
    if (build_room_message(
            outgoing,
            sizeof(outgoing),
            room_name,
            sender_username,
            message) == -1)
    {
        pthread_mutex_unlock(
            &clients_mutex
        );


        (void)send_response(
            sender_fd,
            "ERR 007 MESSAGE_TOO_LONG "
            NID_TAG "\n"
        );


        log_event(
            "ERR 007 RMSG_MESSAGE_TOO_LONG"
        );


        return;
    }


    /*
     * Snapshot all current room-member
     * sockets while holding the mutex.
     */
    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        if (rooms[room_slot].members[index] &&
            clients[index].active &&
            clients[index].registered)
        {
            target_fds[
                target_count++
            ] =
                clients[index].socket_fd;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    /*
     * Deliver only to room members.
     */
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


    /*
     * Required sender response.
     */
    (void)send_response(
        sender_fd,
        "OK SENT " NID_TAG "\n"
    );


    /*
     * Server-side log.
     */
    {
        char log_message[
            LOG_BUFFER_SIZE
        ];


        snprintf(
            log_message,
            sizeof(log_message),
            "RMSG room=%s from=%s message=",
            room_name,
            sender_username
        );


        {
            size_t used =
                strlen(log_message);


            size_t remaining =
                sizeof(log_message)
                - used
                - 1;


            size_t copy_length =
                strlen(message);


            if (copy_length >
                remaining)
            {
                copy_length =
                    remaining;
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


        log_event(
            log_message
        );
    }
}


/* =========================================================
 * Remove a client from every room
 *
 * Empty rooms are removed.
 * ========================================================= */

static void remove_client_from_rooms(
    int slot)
{
    int room_index;

    int member_index;


    pthread_mutex_lock(
        &clients_mutex
    );


    for (room_index = 0;
         room_index < MAX_ROOMS;
         room_index++)
    {
        if (!rooms[room_index].active)
        {
            continue;
        }


        rooms[room_index].members[
            slot
        ] = 0;


        for (member_index = 0;
             member_index < MAX_CLIENTS;
             member_index++)
        {
            if (
                rooms[room_index].members[
                    member_index
                ] &&
                clients[member_index].active &&
                clients[member_index].registered
            )
            {
                break;
            }
        }


        if (member_index ==
            MAX_CLIENTS)
        {
            rooms[room_index].active =
                0;


            rooms[room_index].name[0] =
                '\0';


            memset(
                rooms[room_index].members,
                0,
                sizeof(rooms[room_index].members)
            );
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );
}


/* =========================================================
 * Cleanup one client
 * ========================================================= */

static void cleanup_client(
    int slot)
{
    char username[
        MAX_USERNAME
    ];


    char log_message[
        BUFFER_SIZE
    ];


    int was_registered;


    int client_socket;


    pthread_mutex_lock(
        &clients_mutex
    );


    if (!clients[slot].active)
    {
        pthread_mutex_unlock(
            &clients_mutex
        );

        return;
    }


    client_socket =
        clients[slot].socket_fd;


    was_registered =
        clients[slot].registered;


    strncpy(
        username,
        clients[slot].username,
        sizeof(username) - 1
    );


    username[
        sizeof(username) - 1
    ] = '\0';


    /*
     * Mark client inactive first,
     * then clean room state.
     */
    clients[slot].active =
        0;


    clients[slot].registered =
        0;


    clients[slot].socket_fd =
        -1;


    clients[slot].username[0] =
        '\0';


    pthread_mutex_unlock(
        &clients_mutex
    );


    remove_client_from_rooms(
        slot
    );


    shutdown(
        client_socket,
        SHUT_RDWR
    );


    close(
        client_socket
    );


    if (was_registered)
    {
        snprintf(
            log_message,
            sizeof(log_message),
            "DISCONNECT %s",
            username
        );


        log_event(
            log_message
        );


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


/* =========================================================
 * Client thread
 * ========================================================= */

static void *client_handler(
    void *arg)
{
    int slot;

    int socket_fd;

    int registered_seen = 0;

    char line[BUFFER_SIZE];


    slot =
        *((int *)arg);


    free(arg);


    pthread_mutex_lock(
        &clients_mutex
    );


    socket_fd =
        clients[slot].socket_fd;


    pthread_mutex_unlock(
        &clients_mutex
    );


    while (server_running)
    {
        int result;


        result =
            recv_line(
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
         * Handle CRLF.
         */
        line[
            strcspn(line, "\r")
        ] = '\0';


        /* =================================================
         * REGISTER
         * ================================================= */

        if (!registered_seen)
        {
            const char prefix[] =
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


                int registration_result;


                registration_result =
                    register_client(
                        slot,
                        username
                    );


                if (registration_result == 1)
                {
                    registered_seen =
                        1;


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
                else if (
                    registration_result == -1)
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


        /* =================================================
         * LIST
         * ================================================= */

        if (strcmp(line, "LIST") == 0)
        {
            handle_list(
                socket_fd
            );

            continue;
        }


        /* =================================================
         * BCAST
         * ================================================= */

        if (strncmp(
                line,
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


        /* =================================================
         * PMSG
         * ================================================= */

        if (strncmp(
                line,
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


        /* =================================================
         * JOIN
         * ================================================= */

        if (strncmp(
                line,
                "JOIN ",
                5) == 0)
        {
            handle_join(
                slot,
                socket_fd,
                line + 5
            );

            continue;
        }


        /* =================================================
         * LEAVE
         * ================================================= */

        if (strncmp(
                line,
                "LEAVE ",
                6) == 0)
        {
            handle_leave(
                slot,
                socket_fd,
                line + 6
            );

            continue;
        }


        /* =================================================
         * ROOMS
         * ================================================= */

        if (strcmp(line, "ROOMS") == 0)
        {
            handle_rooms(
                socket_fd
            );

            continue;
        }


        /* =================================================
         * RMSG
         * ================================================= */

        if (strncmp(
                line,
                "RMSG ",
                5) == 0)
        {
            handle_room_message(
                slot,
                socket_fd,
                line + 5
            );

            continue;
        }


        /* =================================================
         * QUIT
         * ================================================= */

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


        /* =================================================
         * Invalid command
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


    cleanup_client(
        slot
    );


    return NULL;
}


/* =========================================================
 * Create server socket
 * ========================================================= */

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


    if (listen(
            fd,
            BACKLOG) == -1)
    {
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

    int index;


    /*
     * Prevent disconnected clients from
     * terminating the server through SIGPIPE.
     */
    signal(
        SIGPIPE,
        SIG_IGN
    );


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
    for (index = 0;
         index < MAX_CLIENTS;
         index++)
    {
        clients[index].socket_fd =
            -1;


        clients[index].active =
            0;


        clients[index].registered =
            0;


        clients[index].username[0] =
            '\0';
    }


    /*
     * Initialise room table.
     */
    memset(
        rooms,
        0,
        sizeof(rooms)
    );


    /*
     * Create server socket.
     */
    listen_fd =
        create_server_socket();


    if (listen_fd == -1)
    {
        return EXIT_FAILURE;
    }


    /*
     * Server log.
     */
    log_event(
        "SERVER_STARTED port=6826"
    );


    /*
     * Startup information.
     */
    printf(
        "============================================================\n"
    );

    printf(
        "              NetMessenger Server\n"
    );

    printf(
        "============================================================\n"
    );

    printf(
        "Listening on TCP port : 6826\n"
    );

    printf(
        "Concurrency model     : thread-per-client (pthread)\n"
    );

    printf(
        "NID                    : NID:5608\n"
    );

    printf(
        "Protocol commands      : REGISTER LIST BCAST PMSG\n"
    );

    printf(
        "                         JOIN LEAVE ROOMS RMSG QUIT\n"
    );

    printf(
        "------------------------------------------------------------\n"
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


        accepted_fd =
            accept(
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


            close(
                accepted_fd
            );


            continue;
        }


        /*
         * Allocate thread argument.
         */
        slot_arg =
            malloc(
                sizeof(*slot_arg)
            );


        if (slot_arg == NULL)
        {
            log_event(
                "MEMORY_ALLOCATION_FAILED"
            );


            cleanup_client(
                slot
            );


            continue;
        }


        *slot_arg =
            slot;


        /*
         * Create detached thread.
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


            free(
                slot_arg
            );


            cleanup_client(
                slot
            );


            continue;
        }


        pthread_detach(
            thread_id
        );
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


    for (index = 0;
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


            clients[index].active =
                0;


            clients[index].registered =
                0;


            clients[index].socket_fd =
                -1;
        }
    }


    pthread_mutex_unlock(
        &clients_mutex
    );


    if (listen_fd != -1)
    {
        close(
            listen_fd
        );

        listen_fd = -1;
    }


    /*
     * Mutexes are intentionally not destroyed here
     * because detached client threads may still be
     * finishing their cleanup while the process exits.
     */

    printf(
        "NetMessenger server stopped.\n"
    );


    return EXIT_SUCCESS;
}
