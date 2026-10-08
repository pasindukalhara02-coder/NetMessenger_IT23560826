/*
 * NetMessenger Client
 * IE3010 Network Programming
 *
 * Student       : Pasindu Kalhara
 * Registration  : IT23560826
 *
 * Server IP     : 127.0.0.1
 * Server Port   : 6826
 * NID           : NID:5608
 *
 * Implemented client-side protocol:
 *   REGISTER
 *   LIST
 *   BCAST
 *   PMSG
 *   JOIN
 *   LEAVE
 *   ROOMS
 *   RMSG
 *   SENDFILE
 *   QUIT
 *
 * HELP is a local client command only.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 6826

#define BUFFER_SIZE 1024
#define MAX_USERNAME 64


/* =========================================================
 * Global state
 * ========================================================= */

static int socket_fd = -1;

static volatile int running = 1;

static volatile int quit_requested = 0;


/* =========================================================
 * Reliable send
 * ========================================================= */

static ssize_t send_all(
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
 * Send one protocol line
 * ========================================================= */

static int send_line(
    const char *line)
{
    char buffer[BUFFER_SIZE];

    int length;


    length = snprintf(
        buffer,
        sizeof(buffer),
        "%s\n",
        line
    );


    if (length < 0 ||
        (size_t)length >= sizeof(buffer))
    {
        printf(
            "Message too long.\n"
        );

        return -1;
    }


    return (int)send_all(
        buffer,
        (size_t)length
    );
}


/* =========================================================
 * Receive one line
 * ========================================================= */

static int recv_line(
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
                 * Drain oversized line.
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
 * Protocol help
 *
 * Local client display only.
 * It does not add a new server protocol command.
 * ========================================================= */

static void print_protocol_help(void)
{
    printf(
        "\n"
        "============================================================\n"
    );

    printf(
        "              NetMessenger Protocol Help\n"
    );

    printf(
        "============================================================\n"
    );

    printf(
        "REGISTER <username>\n"
    );

    printf(
        "    Register a unique username.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "LIST\n"
    );

    printf(
        "    List currently connected users.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "BCAST <message>\n"
    );

    printf(
        "    Send a message to all other connected users.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "PMSG <username> <message>\n"
    );

    printf(
        "    Send a private message to one user.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "JOIN <room>\n"
    );

    printf(
        "    Create or join a chat room.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "LEAVE <room>\n"
    );

    printf(
        "    Leave a chat room.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "ROOMS\n"
    );

    printf(
        "    List currently available rooms.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "RMSG <room> <message>\n"
    );

    printf(
        "    Send a message to members of a room.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "SENDFILE <target> <filename> <filesize>\n"
    );

    printf(
        "    Send a file to a user or room.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "QUIT\n"
    );

    printf(
        "    Disconnect cleanly from the server.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "HELP\n"
    );

    printf(
        "    Display this help again.\n"
    );

    printf(
        "------------------------------------------------------------\n"
    );

    printf(
        "Server OK/ERR responses use: NID:5608\n"
    );

    printf(
        "============================================================\n\n"
    );
}


/* =========================================================
 * Receiver thread
 * ========================================================= */

static void *receiver_thread(
    void *arg)
{
    (void)arg;


    while (running)
    {
        char line[BUFFER_SIZE];

        int result;


        result = recv_line(
            line,
            sizeof(line)
        );


        if (result == 1)
        {
            printf(
                "Server: %s\n",
                line
            );

            fflush(stdout);


            /*
             * Server has confirmed clean
             * disconnection.
             */
            if (strncmp(
                    line,
                    "OK BYE",
                    6) == 0)
            {
                quit_requested = 1;

                running = 0;

                break;
            }


            continue;
        }


        if (result == 0)
        {
            if (running)
            {
                printf(
                    "Server disconnected.\n"
                );
            }


            running = 0;

            break;
        }


        if (result == -2)
        {
            printf(
                "Server sent an oversized line.\n"
            );

            fflush(stdout);

            continue;
        }


        perror("recv");

        running = 0;

        break;
    }


    return NULL;
}


/* =========================================================
 * Main
 * ========================================================= */

int main(void)
{
    struct sockaddr_in server_address;

    char username[MAX_USERNAME];

    char line[BUFFER_SIZE];

    int registered = 0;

    pthread_t receiver;


    /* =====================================================
     * Create socket
     * ===================================================== */

    socket_fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );


    if (socket_fd == -1)
    {
        perror("socket");

        return EXIT_FAILURE;
    }


    /* =====================================================
     * Configure server address
     * ===================================================== */

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
            &server_address.sin_addr) != 1)
    {
        fprintf(
            stderr,
            "Invalid server address.\n"
        );


        close(socket_fd);

        return EXIT_FAILURE;
    }


    /* =====================================================
     * Connect
     * ===================================================== */

    if (connect(
            socket_fd,
            (struct sockaddr *)&server_address,
            sizeof(server_address)) == -1)
    {
        perror("connect");

        close(socket_fd);

        return EXIT_FAILURE;
    }


    printf(
        "============================================================\n"
    );

    printf(
        "              NetMessenger Client\n"
    );

    printf(
        "============================================================\n"
    );

    printf(
        "Connected to NetMessenger server.\n"
    );

    printf(
        "Server: %s:%d\n",
        SERVER_IP,
        SERVER_PORT
    );

    printf(
        "NID: NID:5608\n"
    );

    printf(
        "============================================================\n"
    );


    /* =====================================================
     * Registration
     *
     * IMPORTANT:
     * "OK REGISTERED " is 14 characters.
     * ===================================================== */

    while (!registered)
    {
        int result;


        printf(
            "Server: WELCOME NetMessenger\n"
        );


        printf(
            "Enter username: "
        );


        fflush(stdout);


        if (fgets(
                username,
                sizeof(username),
                stdin) == NULL)
        {
            close(socket_fd);

            return EXIT_FAILURE;
        }


        username[
            strcspn(
                username,
                "\r\n"
            )
        ] = '\0';


        if (username[0] == '\0')
        {
            printf(
                "Username cannot be empty.\n"
            );

            continue;
        }


        snprintf(
            line,
            sizeof(line),
            "REGISTER %s",
            username
        );


        if (send_line(line) == -1)
        {
            perror("send");

            close(socket_fd);

            return EXIT_FAILURE;
        }


        result = recv_line(
            line,
            sizeof(line)
        );


        if (result != 1)
        {
            printf(
                "Registration failed: "
                "server disconnected.\n"
            );


            close(socket_fd);

            return EXIT_FAILURE;
        }


        printf(
            "Server: %s\n",
            line
        );


        /*
         * Correct protocol success check.
         *
         * "OK REGISTERED " = 14 characters.
         *
         * We also make sure that this is genuinely
         * a registration success response.
         */
        if (strncmp(
                line,
                "OK REGISTERED ",
                strlen("OK REGISTERED ")) == 0)
        {
            registered = 1;

            break;
        }


        /*
         * Duplicate username or another
         * registration error.
         */
        printf(
            "Registration was not accepted. "
            "Try another username.\n"
        );
    }


    /* =====================================================
     * Start receiver thread
     * ===================================================== */

    if (pthread_create(
            &receiver,
            NULL,
            receiver_thread,
            NULL) != 0)
    {
        perror("pthread_create");

        close(socket_fd);

        return EXIT_FAILURE;
    }


    /* =====================================================
     * Display protocol commands
     * ===================================================== */

    print_protocol_help();


    /* =====================================================
     * Interactive command loop
     * ===================================================== */

    while (running)
    {
        printf("> ");

        fflush(stdout);


        if (fgets(
                line,
                sizeof(line),
                stdin) == NULL)
        {
            if (running)
            {
                (void)send_line(
                    "QUIT"
                );
            }

            break;
        }


        line[
            strcspn(
                line,
                "\r\n"
            )
        ] = '\0';


        if (line[0] == '\0')
        {
            continue;
        }


        /*
         * HELP is local.
         * It is NOT sent to server.
         */
        if (strcmp(
                line,
                "HELP") == 0)
        {
            print_protocol_help();

            continue;
        }


        /*
         * All official protocol commands
         * are sent unchanged to server.
         */
        if (send_line(line) == -1)
        {
            perror("send");

            running = 0;

            break;
        }


        if (strcmp(
                line,
                "QUIT") == 0)
        {
            quit_requested = 1;

            break;
        }
    }


    /*
     * Allow receiver thread to print
     * OK BYE NID:5608.
     */
    if (quit_requested)
    {
        sleep(1);
    }


    running = 0;


    shutdown(
        socket_fd,
        SHUT_RDWR
    );


    close(socket_fd);


    socket_fd = -1;


    pthread_join(
        receiver,
        NULL
    );


    printf(
        "Client connection closed.\n"
    );


    return EXIT_SUCCESS;
}
