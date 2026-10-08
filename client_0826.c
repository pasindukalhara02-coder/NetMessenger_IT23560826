/*
 * NetMessenger - IE3010 Network Programming
 * Student: Pasindu Kalhara
 * Registration Number: IT23560826
 *
 * Personalisation:
 *   Server IP : 127.0.0.1
 *   Port      : 6826
 *   NID       : NID:5608
 *
 * Implemented client commands:
 *   REGISTER
 *   LIST
 *   BCAST
 *   QUIT
 *
 * More protocol commands will be integrated in later
 * milestones without removing existing functionality.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 6826

#define BUFFER_SIZE 1024
#define MAX_USERNAME 64

static int socket_fd = -1;
static volatile sig_atomic_t connected = 1;


/* =========================================================
 * Reliable send
 * ========================================================= */
static ssize_t send_all(int fd,
                        const void *data,
                        size_t length)
{
    const char *buffer =
        (const char *)data;

    size_t total_sent = 0;


    while (total_sent < length) {

        ssize_t sent =
            send(
                fd,
                buffer + total_sent,
                length - total_sent,
                MSG_NOSIGNAL
            );


        if (sent > 0) {

            total_sent +=
                (size_t)sent;

            continue;
        }


        if (sent == -1 &&
            errno == EINTR) {

            continue;
        }


        return -1;
    }


    return (ssize_t)total_sent;
}


/* =========================================================
 * Send line
 * ========================================================= */
static int send_line(const char *line)
{
    return (int)send_all(
        socket_fd,
        line,
        strlen(line)
    );
}


/* =========================================================
 * Receive one line
 * ========================================================= */
static int recv_line(char *buffer,
                     size_t buffer_size)
{
    size_t position = 0;


    if (buffer_size < 2) {
        return -1;
    }


    while (1) {

        char ch;


        ssize_t received =
            recv(
                socket_fd,
                &ch,
                1,
                0
            );


        if (received == 1) {

            if (ch == '\n') {

                buffer[position] =
                    '\0';

                return 1;
            }


            if (position + 1 <
                buffer_size) {

                buffer[position++] =
                    ch;

            } else {

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
 * Background receiver
 *
 * Receives:
 *   MSG JOIN ...
 *   MSG LEAVE ...
 *   MSG BCAST ...
 *
 * and also displays server responses.
 * ========================================================= */
static void *receiver_thread(void *arg)
{
    char line[BUFFER_SIZE];

    (void)arg;


    while (connected) {

        int result =
            recv_line(
                line,
                sizeof(line)
            );


        if (result == 1) {

            printf(
                "\nServer: %s\n",
                line
            );

            printf("> ");

            fflush(stdout);

            continue;
        }


        connected = 0;

        break;
    }


    return NULL;
}


/* =========================================================
 * Connect to server
 * ========================================================= */
static int connect_to_server(void)
{
    struct sockaddr_in server_address;


    socket_fd =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );


    if (socket_fd == -1) {

        perror("socket");

        return -1;
    }


    memset(
        &server_address,
        0,
        sizeof(server_address)
    );


    server_address.sin_family =
        AF_INET;

    server_address.sin_port =
        htons(SERVER_PORT);


    if (inet_pton(
            AF_INET,
            SERVER_IP,
            &server_address.sin_addr
        ) != 1) {

        fprintf(
            stderr,
            "Invalid server address.\n"
        );

        close(socket_fd);

        socket_fd = -1;

        return -1;
    }


    if (connect(
            socket_fd,
            (struct sockaddr *)&server_address,
            sizeof(server_address)
        ) == -1) {

        perror("connect");

        close(socket_fd);

        socket_fd = -1;

        return -1;
    }


    return 0;
}


/* =========================================================
 * Register user
 *
 * The first protocol command on a new connection must
 * be REGISTER.
 * ========================================================= */
static int register_user(void)
{
    char username[MAX_USERNAME];
    char command[BUFFER_SIZE];
    char response[BUFFER_SIZE];


    while (connected) {

        printf(
            "Enter username: "
        );

        fflush(stdout);


        if (fgets(
                username,
                sizeof(username),
                stdin
            ) == NULL) {

            return -1;
        }


        username[
            strcspn(
                username,
                "\r\n"
            )
        ] = '\0';


        if (username[0] == '\0') {

            printf(
                "Username cannot be empty.\n"
            );

            continue;
        }


        if (snprintf(
                command,
                sizeof(command),
                "REGISTER %s\n",
                username
            ) >= (int)sizeof(command)) {

            printf(
                "Username is too long.\n"
            );

            continue;
        }


        if (send_line(command) == -1) {

            perror("send");

            return -1;
        }


        int result =
            recv_line(
                response,
                sizeof(response)
            );


        if (result == 0) {

            printf(
                "Server disconnected.\n"
            );

            return -1;
        }


        if (result == -1) {

            perror("recv");

            return -1;
        }


        if (result == -2) {

            printf(
                "Server response was too long.\n"
            );

            return -1;
        }


        printf(
            "Server: %s\n",
            response
        );


        if (strncmp(
                response,
                "OK REGISTERED ",
                strlen("OK REGISTERED ")
            ) == 0) {

            return 0;
        }


        if (strncmp(
                response,
                "ERR ",
                4
            ) == 0) {

            printf(
                "Registration failed. "
                "Please try another username.\n"
            );

            continue;
        }


        printf(
            "Unexpected registration response.\n"
        );
    }


    return -1;
}


/* =========================================================
 * Main client
 * ========================================================= */
int main(void)
{
    pthread_t receiver;


    signal(
        SIGPIPE,
        SIG_IGN
    );


    if (connect_to_server() == -1) {

        return EXIT_FAILURE;
    }


    printf(
        "Connected to NetMessenger server.\n"
    );

    printf(
        "Server: %s:%d\n",
        SERVER_IP,
        SERVER_PORT
    );


    /* REGISTER must be the first command. */
    if (register_user() == -1) {

        shutdown(
            socket_fd,
            SHUT_RDWR
        );

        close(socket_fd);

        return EXIT_FAILURE;
    }


    /*
     * Start receiver thread only after successful
     * registration. This prevents registration response
     * and the initial prompt from racing.
     */
    if (pthread_create(
            &receiver,
            NULL,
            receiver_thread,
            NULL
        ) != 0) {

        fprintf(
            stderr,
            "Failed to create receiver thread.\n"
        );

        shutdown(
            socket_fd,
            SHUT_RDWR
        );

        close(socket_fd);

        return EXIT_FAILURE;
    }


    /*
     * Interactive protocol loop.
     *
     * Supported at this milestone:
     *   LIST
     *   BCAST <message>
     *   QUIT
     *
     * Future commands will be added here while keeping
     * the existing commands working.
     */
    while (connected) {

        char command[BUFFER_SIZE];


        printf("> ");

        fflush(stdout);


        if (fgets(
                command,
                sizeof(command),
                stdin
            ) == NULL) {

            break;
        }


        if (send_line(command) == -1) {

            break;
        }


        if (strncmp(
                command,
                "QUIT",
                4
            ) == 0) {

            break;
        }
    }


    connected = 0;


    shutdown(
        socket_fd,
        SHUT_RDWR
    );


    close(socket_fd);


    pthread_join(
        receiver,
        NULL
    );


    printf(
        "Client connection closed.\n"
    );


    return EXIT_SUCCESS;
}
