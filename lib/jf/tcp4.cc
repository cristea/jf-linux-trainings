#include "tcp4.h"

#include "ip4.h"

#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/ip.h>


namespace jf {

// --------------------------------------------------------------------
TCP4Connection::TCP4Connection(
    const std::string& address,
    uint16_t port_number)
{
    IP4Address addr4(address); // might throw

    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1)
        throw SystemError(errno);

    this->own(sock);

    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_number);
    server_addr.sin_addr = addr4;

    int error = ::connect(*this, (const struct sockaddr*)&server_addr, sizeof(server_addr));
    if (error)
        throw SystemError(errno);
}

// --------------------------------------------------------------------
TCP4Port::TCP4Port(uint16_t port_number)
: port_number_(port_number)
{
    port_fd_ = FD(::socket(AF_INET, SOCK_STREAM, 0));
    if (port_fd_ == -1)
        throw SystemError(errno);

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_number_);
    addr.sin_addr.s_addr = INADDR_ANY;

    int error = ::bind(port_fd_, (const struct sockaddr*)&addr, sizeof(addr));
    if (error) {
        if (errno == EADDRINUSE)
            throw EAddrInUse(errno);
        else
            throw SystemError(errno);
    }

    error = ::listen(port_fd_, SOMAXCONN);
    if (error)
        throw SystemError(errno);
}

TCP4Connection TCP4Port::accept()
{
    int connection = ::accept(port_fd_, nullptr, nullptr);
    if (connection == -1)
        throw SystemError(errno);
    return TCP4Connection(connection);
}

}
