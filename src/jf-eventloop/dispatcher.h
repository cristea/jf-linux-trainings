#ifndef JF_LINUXISH__JF_EVENTLOOP__DISPATCHER_H
#define JF_LINUXISH__JF_EVENTLOOP__DISPATCHER_H

#include <map>
#include <functional>

namespace jf {
namespace linuxish {

class Dispatcher
{
public:
    using Handler = std::function<void(int/*fd*/)>;
    
public:
    ~Dispatcher();

    void watch_in(int fd, Handler);
    void watch_out(int fd, Handler);

    void unwatch_in(int fd);
    void unwatch_out(int fd);

    void dispatch();

private:
    // should probably use hash_map instead, but gcc keeps giving me a
    // backward_warning. unordered_map, on the other hand, gives
    // c++0x_warning.h
    typedef std::map<int, Handler> HandlerSet;
    HandlerSet in_handlers_;
    HandlerSet out_handlers_;
};

}
}

#endif