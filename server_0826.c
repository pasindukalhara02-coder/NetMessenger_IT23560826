/*
 * NetMessenger - IE3010 Network Programming
 * Student: Pasindu Kalhara
 * Registration Number: IT23560826
 *
 * Server:
 *   Source file : server_0826.c
 *   Port        : 6826
 *   NID         : NID:5608
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
#include <time.h>
#include <unistd.h>

#define SERVER_PORT 6826
#define BACKLOG 10
#define BUFFER_SIZE 1024
#define LOG_FILE "netmsg_IT23560826.log"

static volatile sig_atomic_t server_running = 1;
static int listen_fd = -1;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Signal handler for graceful server shutdown.
 */
static void handle_signal(int signal_number)
{
    (void)signal_number;
    server_running = 0;

    if (listen_fd != -1) {
        close(listen_fd);
        listen_fd = -1;
    }
}

/*
 * Write a timestamped event to the personalised server log.
 */
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

    if (strftime(timestamp, sizeof(timestamp),
                 "%Y-%m-%d %H:%M:%S",
                 &time_info) == 0) {
        return;
    }

    pthread_mutex_lock(&log_mutex);

    log_file = fopen(LOG_FILE, "a");
    if (log_file != NULL) {
        fprintf(log_file, "[%s] %s\n", timestamp, event);
        fclose(log_file);
    }

    pthread_mutex_unlock(&log_mutex);
}

/*
 * Handle one connected client.
 *
 * This is the foundation for the required thread-per-client
 * concurrency model. Protocol command processing will be added
 * incrementally in later stages.
 */
static void *client_handler(void *arg)
{
    int client_fd;
    struct sockaddr_in client_address;
    socklen_t address_length;
    char client_ip[INET_ADDRSTRLEN];
    char log_message[256];
    char buffer[BUFFER_SIZE];

    client_fd = *((int *)arg);
    free(arg);

    address_length = sizeof(client_address);

    memset(&client_address, 0, sizeof(client_address));

    if (getpeername(client_fd,
                    (struct sockaddr *)&client_address,
                    &address_length) == 0) {

        if (inet_ntop(AF_INET,
                      &client_address.sin_addr,
                      client_ip,
                      sizeof(client_ip)) == NULL) {
            snprintf(client_ip, sizeof(client_ip), "unknown");
        }

        snprintf(log_message,
                 sizeof(log_message),
                 "CLIENT_CONNECTED %s:%u",
                 client_ip,
                 (unsigned int)ntohs(client_address.sin_port));

    } else {
        snprintf(log_message,
                 sizeof(log_message),
                 "CLIENT_CONNECTED unknown");
    }

    log_event(log_message);

    while (server_running) {
        ssize_t received = recv(client_fd,
                                buffer,
                                sizeof(buffer),
                                0);

        if (received > 0) {
            snprintf(log_message,
                     sizeof(log_message),
                     "CLIENT_DATA_RECEIVED %zd bytes",
                     received);

            log_event(log_message);
            continue;
        }

        if (received == 0) {
            break;
        }

        if (errno == EINTR) {
            continue;
        }

        snprintf(log_message,
                 sizeof(log_message),
                 "CLIENT_RECV_ERROR %s",
                 strerror(errno));

        log_event(log_message);
        break;
    }

    log_event("CLIENT_DISCONNECTED");

    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);

    return NULL;
}

/*
 * Create, configure, bind, and listen on the TCP server socket.
 */
static int create_server_socket(void)
{
    int fd;
    int reuse_address = 1;
    struct sockaddr_in server_address;

    fd = socket(AF_INET, SOCK_STREAM, 0);

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

    memset(&server_address, 0, sizeof(server_address));

    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(SERVER_PORT);

    if (bind(fd,
             (struct sockaddr *)&server_address,
             sizeof(server_address)) == -1) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, BACKLOG) == -1) {
        perror("listen");
        close(fd);
        return -1;
    }

    return fd;
}

int main(void)
{
    struct sigaction signal_action;

    memset(&signal_action, 0, sizeof(signal_action));
    signal_action.sa_handler = handle_signal;
    sigemptyset(&signal_action.sa_mask);

    if (sigaction(SIGINT, &signal_action, NULL) == -1) {
        perror("sigaction");
        return EXIT_FAILURE;
    }

    listen_fd = create_server_socket();

    if (listen_fd == -1) {
        return EXIT_FAILURE;
    }

    log_event("SERVER_STARTED port=6826");

    printf("NetMessenger server started.\n");
    printf("Listening on TCP port: 6826\n");
    printf("Concurrency model: thread-per-client (pthread)\n");
    printf("Press Ctrl+C to stop the server.\n");

    while (server_running) {
        struct sockaddr_in client_address;
        socklen_t client_length;
        int accepted_fd;
        int *client_fd;
        pthread_t thread_id;

        client_length = sizeof(client_address);

        accepted_fd = accept(listen_fd,
                             (struct sockaddr *)&client_address,
                             &client_length);

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

        client_fd = malloc(sizeof(*client_fd));

        if (client_fd == NULL) {
            fprintf(stderr, "Memory allocation failed for client.\n");
            close(accepted_fd);
            continue;
        }

        *client_fd = accepted_fd;

        if (pthread_create(&thread_id,
                           NULL,
                           client_handler,
                           client_fd) != 0) {
            fprintf(stderr, "Failed to create client thread.\n");
            free(client_fd);
            close(accepted_fd);
            continue;
        }

        pthread_detach(thread_id);
    }

    log_event("SERVER_STOPPED");

    if (listen_fd != -1) {
        close(listen_fd);
        listen_fd = -1;
    }

    pthread_mutex_destroy(&log_mutex);

    printf("NetMessenger server stopped.\n");

    return EXIT_SUCCESS;
}
