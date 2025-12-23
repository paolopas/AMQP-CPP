/*
 * This software is the product of voluntary contributions, which Copernica BV
 * distributes alongside with the proprietary code for the sole purpose of
 * allowing its circulation, and is subject to the same license terms:
 *
 * Copyright 2025 Copernica BV
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Specifically, regarding the code contained in this file, Copernica BV
 * IS NOT INVOLVED IN ANY ACTIVITY BEYOND DISTRIBUTION. IT DOES NOT PROVIDE
 * TECHNICAL SUPPORT, MAINTENANCE, OR ANY OTHER SERVICES.
 *
 *
 *  LibBoostAsio.h
 *
 *  Implementation for the AMQP::TcpHandler for boost::asio. You can use this class
 *  instead of a AMQP::TcpHandler class, just pass the boost asio io_context to the
 *  constructor and you're all set.  See examples/libboostasio.cpp for an example.
 *
 *  Watch out: this class was not implemented or reviewed by the original author of
 *  AMQP-CPP. If you run into some issues, it might be better to implement your own
 *  handler that interact with boost.
 *
 *
 *  @author Gavin Smith <gavin.smith@coralbay.tv>
 *  @author Paolo Pastori
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <memory>
#include <chrono>
#include <functional>
#include <algorithm>
#include <cassert>

#include <boost/asio/io_context.hpp>
#include <boost/asio/io_context_strand.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/dispatch.hpp>

#include "amqpcpp/linux_tcp.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 *  @note Because of a limitation on Windows, this will only work on POSIX based systems - see https://github.com/chriskohlhoff/asio/issues/70
 */
class LibBoostAsioHandler : public TcpHandler
{
protected:

    /**
     *  Helper class that wraps a boost io_context socket monitor.
     */
    class Watcher
    {
    private:

        /**
         *  The parent Handler.
         *  @var LibBoostAsioHandler
         */
        LibBoostAsioHandler *_parent;

        /**
         *  The boost tcp socket.
         *  @var class boost::asio::ip::tcp::socket
         *  @note https://stackoverflow.com/questions/38906711/destroying-boost-asio-socket-without-closing-native-handler
         */
        boost::asio::posix::stream_descriptor _socket;

        /**
         *  The boost asynchronous timer.
         *
         *  Used for monitoring of both server connection timeout condition
         *  (at initial connection stage) and heartbeat (client and server).
         *  @var boost::asio::steady_timer
         */
        boost::asio::steady_timer _timer;

        /**
         *  AMQP Server connection timeout setting (seconds).
         *  @var uint16_t
         */
        uint16_t _connection_timeout = 0;

        /**
         *  Timeout after which the connection is no longer considered alive.
         *  A heartbeat must be sent every _timeout / 2 seconds.
         *  Value zero means heartbeats are disabled, or not yet negotiated.
         *  @var uint16_t
         */
        uint16_t _timeout = 0;

        using steady_time_point = std::chrono::time_point<std::chrono::steady_clock>;

        /**
         *  When should we send the next heartbeat?
         *  @var std::chrono::time_point<std::chrono::steady_clock>
         */
        steady_time_point _next;

        /**
         *  When does the connection expire / was the server idle for too long?
         *  @var std::chrono::time_point<std::chrono::steady_clock>
         */
        steady_time_point _expire;

        /**
         *  The events for which the socket filedescriptor is actually monitored.
         *  @var int
         */
        int _events = 0;

        /**
         *  A boolean that indicates if the watcher is monitoring for read events.
         *  @var _read True if reads are being monitored else false.
         */
        bool _read = false;

        /**
         *  A boolean that indicates if the watcher is monitoring for write events.
         *  @var _write True if writes are being monitored else false.
         */
        bool _write = false;

        using boost_errc = boost::system::error_code;
        using handler_cb = std::function<void(boost_errc)>;

        /**
         *  Make a generic handler callback.
         *  @param  mmfn        The wrapping member pointer to be invoked by the handler.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         *  @return handler_cb
         */
        template <typename M>
        handler_cb make_handler(M &&mmfn, TcpConnection *const connection, const int fd)
        {
            return
#if __cplusplus >= 201402L
                // C++14 lambda has init capture
                [this, mmfn=std::forward<M>(mmfn), connection, fd] (const boost_errc& ec)
#else
                [this, mmfn, connection, fd] (const boost_errc& ec)
#endif
                {
                    if (ec) {
                        // upon object destruction, all callbacks are invoked with
                        // the error code boost::asio::error::operation_aborted,
                        // but regardless of the error, you should exit immediately
                        // because "this" is likely to be invalidated.
                        return;
                    }
                    // here, we are guaranteed by design that the "this"
                    // will not dangle without our intervention
                    boost::asio::dispatch(this->_parent->_strand,
                        std::bind(std::move(mmfn), this, connection, fd));
                };
        }

        /**
         *  Handler method that is called by boost's io_context when the socket pumps a read event.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         */
        void read_handler(TcpConnection *const connection,
                          const int fd)
        {
            if (_read)
            {
                if (_timeout)
                {
                    // the server is sending data, update the _expire time
                    _expire = std::chrono::steady_clock::now() +
                            std::chrono::seconds(_timeout + (_timeout >> 1) + 1);
                }

                connection->process(fd, AMQP::readable);
                // Beware, the library may have triggered the watcher destruction

                // still we need monitoring read?
                if (_socket.is_open())
                {
                    _socket.async_wait(
                        boost::asio::posix::stream_descriptor::wait_read,
                        get_read_handler(connection, fd));
                }
            }
        }
        /**
         *  Make a handler callback to invoke the read_handler method.
         */
        handler_cb get_read_handler(TcpConnection *const connection, const int fd)
        {
            return make_handler(std::mem_fn(&Watcher::read_handler), connection, fd);
        }

        /**
         *  Handler method that is called by boost's io_context when the socket pumps a write event.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         */
        void write_handler(TcpConnection *const connection,
                           const int fd)
        {
            if (_write)
            {
                if (_timeout)
                {
                    // the client is sending data, update the _next time
                    _next = std::chrono::steady_clock::now() +
                            std::chrono::seconds((_timeout >> 1) + 1);
                }

                connection->process(fd, AMQP::writable);
                // Beware, the library may have triggered the watcher destruction

                // still we need monitoring write?
                if (_socket.is_open())
                {
                    _socket.async_wait(
                        boost::asio::posix::stream_descriptor::wait_write,
                        get_write_handler(connection, fd));
                }
            }
        }
        /**
         *  Make a handler callback to invoke the write_handler method.
         */
        handler_cb get_write_handler(TcpConnection *const connection, const int fd)
        {
            return make_handler(std::mem_fn(&Watcher::write_handler), connection, fd);
        }

        /**
         *  Handler method that is called by boost's io_context when the timer expires.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         */
        void timer_handler(TcpConnection *const connection,
                           const int fd)
        {
            if (_socket.is_open())
            {
                steady_time_point now = std::chrono::steady_clock::now();

                if (_timeout == 0)
                {
                    // this can happen in three situations:
                    // 1. a connection timeout,
                    // 2. user space has overidden onNegotiate to reject heartbeats
                    // 3. AMQP server does not want heartbeats
                    // in either case we're no longer going to run further timers.

                    // if we have an initialized connection, user space must have overidden
                    // the onNegotiate method, so we keep using the connection
                    if (connection->initialized()) return;

                    // this is a connection timeout, close the connection with immediate effect
                    return (void) connection->close(true);
                }
                else if (now >= _expire)
                {
                    // the server was inactive for a too long period of time,
                    // close the connection with immediate effect
                    _timeout = 0;
                    return (void) connection->close(true);
                }
                else if (now >= _next)
                {
                    // send the heartbeat
                    connection->heartbeat();

                    // when we should send out the next one
                    _next = now + std::chrono::seconds((_timeout >> 1) + 1);
                }

                // reschedule the timer
                _timer.expires_at(_next);

                // Posts the timer event
                _timer.async_wait(get_timer_handler(connection, fd));
            }
        }
        /**
         *  Make a handler callback to invoke the timer_handler method.
         */
        handler_cb get_timer_handler(TcpConnection *const connection, const int fd)
        {
            return make_handler(std::mem_fn(&Watcher::timer_handler), connection, fd);
        }

    public:

        /**
         *  Constructor - initialises the watcher and assigns the filedescriptor to
         *  a boost socket for monitoring.
         *  @param  io_context           The boost io_context
         *  @param  parent               The parent Handler
         *  @param  fd                   The filedescriptor being watched
         *  @param  connection_timeout   The AMQP server connection timeout
         */
        Watcher(boost::asio::io_context &io_context,
                LibBoostAsioHandler *parent,
                const int fd,
                uint16_t connection_timeout) :
            _parent(parent),
            _socket(io_context),
            _timer(io_context),
            _connection_timeout(connection_timeout)
        {
            _socket.assign(fd);

            _socket.non_blocking(true);
        }

        /**
         *  Watchers cannot be copied or moved
         *
         *  @param  that    The object to not move or copy
         */
        Watcher(Watcher &&that) = delete;
        Watcher(const Watcher &that) = delete;

        /**
         *  Destructors
         */
        void close()
        {
            // release ownership of filedescriptor and cancel pending io callbacks
            _socket.release();
            // cancel any pending timer callback
            stop_timer();
        }

        ~Watcher() { close(); }

        /**
         *  Set the events for which the filedescriptor is monitored
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descripter being watched.
         *  @param  events      The events to monitor (readable, writable or both)
         */
        void set_event_mask(TcpConnection *const connection, int fd, int events)
        {
            if (events != _events)
            {
                // cancel pending io callback
                _socket.cancel();

                // handle reads?
                _read = ((events & AMQP::readable) != 0);

                if (_read)
                {
                    _socket.async_wait(
                        boost::asio::posix::stream_descriptor::wait_read,
                        get_read_handler(connection, fd));
                }

                // handle writes?
                _write = ((events & AMQP::writable) != 0);

                if (_write)
                {
                    _socket.async_wait(
                        boost::asio::posix::stream_descriptor::wait_write,
                        get_write_handler(connection, fd));
                }

                // remember current events
                _events = events;
            }
        }

        /**
         *  Apply AMQP server connection timeout watching.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descripter being watched.
         */
        void apply_connection_timeout(TcpConnection *const connection, const int fd)
        {
            if (_connection_timeout && !_timeout)
            {
                // schedule the timer
                _timer.expires_after(std::chrono::seconds(_connection_timeout));

                // Posts the timer event
                _timer.async_wait(get_timer_handler(connection, fd));
            }
        }

        /**
         *  Configure the heartbeat interval to use
         *  @param timeout      The timeout value (seconds, 0 to disable heartbeat monitor.)
         */
        void set_heartbeat(uint16_t timeout)
        {
            _timeout = timeout;
        }

        /**
         *  Schedule the timer
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descripter being watched.
         */
        void set_timer(TcpConnection *const connection, const int fd)
        {
            // stop timer in case it was already set
            stop_timer();

            if (_timeout)
            {
                // when we should send out the next heartbeat
                _next = std::chrono::steady_clock::now() + std::chrono::seconds((_timeout >> 1) + 1);
                // by when we should expect some server activity
                _expire = _next + std::chrono::seconds(_timeout);

                // schedule the timer
                _timer.expires_at(_next);

                // Posts the timer event
                _timer.async_wait(get_timer_handler(connection, fd));
            }
        }

        /**
         *  Stop the timer
         */
        void stop_timer()
        {
            // do nothing if it was never set
            _timer.cancel();
        }
    };

    /**
     *  The boost asio io_context.
     *  @var class boost::asio::io_context&
     */
    boost::asio::io_context & _iocontext;

    /**
     *  The boost asio strand.
     *  @var boost::asio::io_context::strand
     */
    boost::asio::io_context::strand _strand;

    /**
     *  Active I/O watchers, indexed by their filedescriptor.
     *  @var std::map<int, Watcher>
     */
    std::map<int, std::unique_ptr<Watcher>> _watchers;

    /**
     *  AMQP Server connection timeout setting (seconds).
     *  @var uint16_t
     */
    uint16_t _connection_timeout;

    /**
     *  Method that is called by AMQP-CPP to register a filedescriptor for readability or writability
     *  @param  connection  The TCP connection object that is reporting
     *  @param  fd          The filedescriptor to be monitored
     *  @param  flags       Should the object be monitored for readability or writability?
     */
    void monitor(TcpConnection *const connection,
                 const int fd,
                 const int flags) override
    {
        // do we already have this filedescriptor
        auto iter = _watchers.find(fd);

        // was it found?
        if (iter == _watchers.end())
        {
            // a new watcher is required

            // should have some flags, unless there was an early release
            if (flags == 0) return;

            // construct a new watcher, and register as active
            _watchers[fd] =
#if __cplusplus >= 201402L
            // C++14 has make_unique(
                std::make_unique<Watcher>(_iocontext, this, fd, _connection_timeout);
#else
                std::unique_ptr<Watcher>(new Watcher(_iocontext, this, fd, _connection_timeout));
#endif

            auto &upwatcher = _watchers[fd];

            // apply server connection timeout monitor
            upwatcher->apply_connection_timeout(connection, fd);

            // explicitly set the events to monitor
            upwatcher->set_event_mask(connection, fd, flags);
        }
        else if (flags == 0)
        {
            // the watcher does not need anymore, unregister
            _watchers.erase(iter);
        }
        else
        {
            // reconfigure the events to monitor
            iter->second->set_event_mask(connection, fd, flags);
        }
    }

protected:

    /**
     *  Method that is called when the heartbeat frequency is negotiated.
     *  @param  connection      The connection that suggested a heartbeat timeout
     *  @param  timeout         The suggested timeout from the server (seconds)
     *  @return uint16_t        The timeout to use
     */
    uint16_t onNegotiate(TcpConnection *connection, uint16_t timeout) override
    {
        // skip if no heartbeats are needed
        if (timeout == 0) return 0;

        const int fd = connection->fileno();

        auto iter = _watchers.find(fd);
        assert(iter != _watchers.end()); // a watcher must exist when negotiating the heartbeat

        // apply heartbeat monitor
        iter->second->set_heartbeat(timeout);
        iter->second->set_timer(connection, fd);

        // we agree with the timeout
        return timeout;
    }

    /**
     *  Stop handling connection(s).
     *
     *  Note that you cannot continue using the connection(s) after calling
     *  this method.
     *  @param  connection  The TcpConnection to release (all by default).
     *  @return bool        If there was a release.
     */
    bool release(const TcpConnection *connection = nullptr)
    {
        if (connection == nullptr)
        {
            // close all watchers
            for (auto &wp : _watchers) wp.second->close();
            auto b = _watchers.begin();
            return b != _watchers.erase(b, _watchers.end());
        }
        else
        {
            int fd = connection->fileno();
            // is the connection already closed?
            if (fd != -1) {
                // close matching watcher
                auto iter = _watchers.find(fd);
                if (iter != _watchers.end())
                {
                    iter->second->close();
                    _watchers.erase(iter);
                    return true;
                }
            }
            return false;
        }
    }

public:

    /**
     *  Handler cannot be default constructed.
     */
    LibBoostAsioHandler() = delete;

    /**
     *  Constructor
     *  @param  io_context          The boost io_context to wrap
     *  @param  connection_timeout  The AMQP server connection timeout (seconds)
     */
    explicit LibBoostAsioHandler(boost::asio::io_context &io_context,
                                 uint16_t connection_timeout = 60) :
        _iocontext(io_context),
        _strand(io_context),
        _connection_timeout(connection_timeout)
    {
    }

    /**
     *  Handler cannot be copied or moved
     *
     *  @param  that    The object to not move or copy
     */
    LibBoostAsioHandler(LibBoostAsioHandler &&that) = delete;
    LibBoostAsioHandler(const LibBoostAsioHandler &that) = delete;

    /**
     *  Returns a reference to the boost io_context object that is being used.
     *  @return The boost io_context object.
     */
    boost::asio::io_context &service()
    {
        return _iocontext;
    }

    /**
     *  Destructor
     */
    ~LibBoostAsioHandler() override
    {
        release();
    }
};

/**
 *  End of namespace
 */
}
/*
 * [1] Regarding the peculiarities of this handler.
 *     ============================================
 *
 * To avoid confusion here I use the term callback to refer to the completion
 * handler of boost asio, while I speak of handler with reference to the
 * LibBoostAsioHandler.
 *
 * This handler is inspired by the others that preceded it (i.e. LibEvHandler,
 * LibEventHandler, LibUvHandler), but the strategy used to monitor the socket
 * filedescriptor is completely different.  While other handlers rely on calls
 * like select/poll or similar to be woken up when a filedescriptor becomes
 * readable/writable, this one uses boost asio's async_wait.
 *
 * Since callbacks queued in the execution context are not automatically
 * requeued once executed, additional work is required to continue monitoring
 * the socket's readability/writableness.  This is not necessary with the other
 * approaches, furthermore with select/poll/etc it is the operating system that
 * checks multiple conditions *simultaneously*, whereas here we are also forced
 * to manage readable and writeable conditions separately.
 *
 * It is therefore essential that when the library requires monitoring a file
 * descriptor being read, there is always a callback queued in the execution
 * context (or executing) that takes care of it.  This condition is signaled by
 * the _read flag of the Watcher which takes care of the relative
 * filedescriptor.  The same goes for writing.
 *
 * Probably even more importantly, this callback must not only be there but
 * also be UNIQUE.  In the past, this handler had many problems related to the
 * simultaneous existence of multiple callbacks monitoring the same condition
 * on the same filedescriptor.  Typically this happened with TLS connections.
 *
 * However, there are also cases where a single callback causes problems,
 * because it simply shouldn't exist.  To understand these cases, consider that
 * the boost reactor doesn't terminate execution if there are queued callbacks.
 * In short, io_context::run() doesn't return unless we clear all callbacks.
 * Under these conditions, a program refuses to terminate and hangs.
 *
 * So it's important to make sure we clear out all callbacks as soon as
 * they're no longer needed.
 *
 * Ensuring that there aren't too many callbacks is definitely the handler's
 * responsibility.  But deleting them all at the end may in some cases require
 * the cooperation of the handler's user, at this purpose see the handler's
 * release method.
 *
 *
 * [2] About LibBoostAsio and thread safety.
 *     =====================================
 *
 * AMQP-CPP is not thread safe, we cannot repeat this too many times.
 *
 * But I think we should definitely give credit to Gavin Smith, the original
 * author of this handler, for the intuition that with the help of the boost
 * reactor it could get pretty close.  The reactor ensures that queued
 * callbacks are executed exclusively by threads that are running the context
 * (io_context::run()) and the io_context::strand ensures that each callback
 * is executed sequentially and every other thread in the pool sees its effects.
 *
 * So boost magically makes AMQP-CPP thread safe?
 *
 * IF ALL THE THREADS INVOLVED ARE RUNNING THE CONTEXT THEN ALL THE CALLS TO
 * AMQP-CPP ORIGINATE FROM CALLBACKS AND THEREFORE THERE ARE NO PROBLEMS
 *                                   BUT
 * IF A THREAD INTERACTS WITH THE LIBRARY WITHOUT GOING THROUGH THE REACTOR
 * (for example it directly invoke publish) THEN EXPECT AN INDETERMINATE
 * BEHAVIOR.
 *                                                             Paolo
 */
