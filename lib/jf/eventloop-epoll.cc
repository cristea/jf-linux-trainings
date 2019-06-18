#include "eventloop-epoll.h"

#include <cassert>
#include <cstring>


namespace jf {

EventLoop_epoll::EventLoop_epoll()
: epoll_fd_(::epoll_create1(0))
{}

void EventLoop_epoll::watch_in(
    int fd,
    EventLoop::Handler h)
{
    int op;
    Notifiers::iterator it = make_notifier_(fd, op);

    Notifier& notifier = it->second;

    assert(!notifier.in_handler);
    notifier.in_handler = h;

    epoll_event event = make_event_(it);
    int error = ::epoll_ctl(epoll_fd_, op, fd, &event);
    assert(!error);
}

void EventLoop_epoll::watch_out(
    int fd, 
    EventLoop::Handler h)
{
    int op;
    Notifiers::iterator it = make_notifier_(fd, op);

    Notifier& notifier = it->second;

    assert(!notifier.out_handler);
    notifier.out_handler = h;

    epoll_event event = make_event_(it);
    int error = ::epoll_ctl(epoll_fd_, op, fd, &event);
    assert(!error);
}

void EventLoop_epoll::unwatch_in(
    int fd)
{
    auto found = notifiers_.find(fd);
    assert(found != notifiers_.end());

    Notifier& notifier = found->second;
    notifier.in_handler = nullptr;

    update_(found);
}

void EventLoop_epoll::unwatch_out(
    int fd)
{
    auto found = notifiers_.find(fd);
    assert(found != notifiers_.end());

    Notifier& notifier = found->second;
    notifier.out_handler = nullptr;

    update_(found);
}

void EventLoop_epoll::run_one()
{
    events_.resize(notifiers_.size());
    int num_ready = ::epoll_wait(epoll_fd_, &events_[0], events_.size(), 1);
    assert(num_ready != -1);

    for (int i=0; i<num_ready; i++) {
        int fd = events_[i].data.fd;

        if (events_[i].events & EPOLLIN) {
            auto found = notifiers_.find(fd);
            if (found != notifiers_.end())
                found->second.in_handler(fd, this);
        }
        if (events_[i].events & EPOLLOUT) {
            auto found = notifiers_.find(fd);
            if (found != notifiers_.end())
                found->second.out_handler(fd, this);
        }
    }
}

epoll_event EventLoop_epoll::make_event_(
    Notifiers::iterator it)
{
    int fd = it->first;
    Notifier& notifier = it->second;
    epoll_event event;

    memset(&event, 0, sizeof(event));
    if (notifier.in_handler)
        event.events |= EPOLLIN;
    if (notifier.out_handler)
        event.events |= EPOLLOUT;
    event.data.fd = fd;

    return event;
}

EventLoop_epoll::Notifiers::iterator EventLoop_epoll::make_notifier_(
    int fd,
    int& epoll_op)
{
    auto it = notifiers_.insert(std::make_pair(fd, Notifier()));
    epoll_op = it.second? EPOLL_CTL_ADD: EPOLL_CTL_MOD;
    return it.first;
}

void EventLoop_epoll::update_(
    Notifiers::iterator it)
{
    int fd = it->first;
    Notifier& notifier = it->second;

    if (notifier.in_handler || notifier.out_handler) {
        epoll_event event = make_event_(it);
        int error = ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event);
        assert(!error);
    }
    else {
        int error = ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        assert(!error);

        notifiers_.erase(it);
    }
}

}
