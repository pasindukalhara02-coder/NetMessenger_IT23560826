/*
 * NetMessenger - IE3010 Network Programming
 * Student: Pasindu Kalhara
 * Registration Number: IT23560826
 *
 * Client:
 *   Source file : client_0826.c
 *   Server port : 6826
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define SERVER_PORT 6826
#define SERVER_IP "127.0.0.1"

int main(void)
{
    int socket_fd;
    struct sockaddr_in server_address;

    socket_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (socket_fd == -1) {
        perror("socket");
        return EXIT_FAILURE;
    }

    memset(&server_address, 0, sizeof(server_address));

    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(SERVER_PORT);

    if (inet_pton(AF_INET,
                  SERVER_IP,
                  &server_address.sin_addr) != 1) {
        fprintf(stderr, "Invalid server address.\n");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    if (connect(socket_fd,
                (struct sockaddr *)&server_address,
                sizeof(server_address)) == -1) {
        perror("connect");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    printf("Connected to NetMessenger server.\n");
    printf("Server: %s:%d\n", SERVER_IP, SERVER_PORT);
    printf("Press Enter to disconnect.\n");

    getchar();

    shutdown(socket_fd, SHUT_RDWR);
    close(socket_fd);

    printf("Client connection closed.\n");

    return EXIT_SUCCESS;
}
