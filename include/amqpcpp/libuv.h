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
 *  LibUV.h
 *
 *  Implementation for the AMQP::TcpHandler that is optimized for libuv. You can
 *  use this class instead of a AMQP::TcpHandler class, just pass the event loop
 *  to the constructor and you're all set.  See examples/libuv.cpp for an example.
 *
 *
 *  @author David Nikdel <david@nikdel.com>
 *  @author Paolo Pastori
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <uv.h>
#if __cplusplus >= 201402L
// >= C++14, using make_unique
#include <memory>
#endif
#include <type_traits>
#include <map>
#include <cassert>

#include "amqpcpp/linux_tcp.h"


// TODO: maybe for AMQP-CPP VERSION > 4.3.27 we can use
//       these in place of the assert placed into ctor
//static_assert(AMQP::readable == UV_READABLE, "libuv vs AMQP-CPP flags mismatch");
//static_assert(AMQP::writable == UV_WRITABLE, "libuv vs AMQP-CPP flags mismatch");

/**
 *  Set up auxiliary namespace
 */
namespace UV_AUX {

    /*
     * Helper template function for getting libuv handle file descriptor,
     * see https://docs.libuv.org/en/v1.x/handle.html#c.uv_fileno
     * @return int
     */
    template <typename H>
    int handle_fileno(H *handle)
    {
        int fd = -1;
        (void) uv_fileno(reinterpret_cast<uv_handle_t*>(handle), &fd);
        return fd;
    }

    /*
     * Helper template function for closing libuv handles,
     * see https://docs.libuv.org/en/v1.x/handle.html#c.uv_close
     */
    template <typename H,
         typename = std::enable_if_t<std::is_same<H, uv_poll_t>::value || std::is_same<H, uv_timer_t>::value || std::is_same<H, uv_signal_t>::value > >
    void close_handle(H *handle)
    {
        uv_close(reinterpret_cast<uv_handle_t*>(handle), [](uv_handle_t* hndl) {
            // delete memory once closed
            delete reinterpret_cast<H*>(hndl);
        });
    }
}


/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class LibUvHandler : public TcpHandler
{
private:
    /**
     *  Helper class that wraps a libuv I/O watcher
     */
    class Watcher
    {
    private:
        /**
         *  The connection being watched
         *  @var TcpConnection
         */
        TcpConnection *_connection;

        /**
         *  The event loop to which it is attached
         *  @var uv_loop_t
         */
        uv_loop_t *_loop;

        /**
         *  The asynchronous io handle
         *  @var uv_poll_t
         */
        uv_poll_t *_poll;

        /**
         *  The asynchronous timer handle
         *  @var uv_timer_t
         */
        uv_timer_t *_timer;

        /**
         *  AMQP Server connection timeout setting (seconds)
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

        /**
         *  When should we send the next heartbeat?
         *  @var uint64_t
         */
        uint64_t _next;

        /**
         *  When does the connection expire / was the server idle for too long?
         *  @var uint64_t
         */
        uint64_t _expire;

        /**
         *  The events for which the filedescriptor is actually monitored.
         *  @var int
         */
        int _events = 0;

        /**
         *  Callback method that is called by libuv when a filedescriptor becomes active
         *  @param  handle   io handle
         *  @param  status   LibUV error code UV_*, see https://docs.libuv.org/en/v1.x/errors.html
         *  @param  events   Events triggered
         */
        static void io_callback(uv_poll_t *handle, int status, int events)
        {
            Watcher *self = static_cast<Watcher*>(handle->data);

            // is the socket still open?
            int fd = ::UV_AUX::handle_fileno(handle);
            if (fd != -1)
            {
                // if an error happens while polling, status will be < 0
                // and corresponds with one of the UV_E* error codes
                if (status)
                {
//std::cerr << "LibUvHandler::Watcher::io_callback() error UV_" << uv_err_name(status) << ", events " << events << ", fd " << fd << '\n';
                    // report rw condition to library to pick up the error
                    events = AMQP::readable | AMQP::writable;
                }
                else
                {
                    if (events & AMQP::readable)
                    {
                        // the server is sending data, update the _expire time
                        self->_expire = uv_now(self->_loop) + self->_timeout * 1500;
                    }
                    if (events & AMQP::writable)
                    {
                        // the client is sending data, update the _next time
                        self->_next = uv_now(self->_loop) + self->_timeout * 500;
                    }
                }

                self->_connection->process(fd, events);
            }
        }

        /**
         *  Callback method that is called by libuv when a timer fires
         *  @param  handle   timer handle
         */
        static void timer_callback(uv_timer_t *handle)
        {
            Watcher *self = static_cast<Watcher*>(handle->data);

            // is the socket still open?
            int fd = ::UV_AUX::handle_fileno(self->_poll);
            if (fd != -1)
            {
                uint64_t now = uv_now(self->_loop);

                if (self->_timeout == 0)
                {
                    // this can happen in three situations:
                    // 1. a connection timeout,
                    // 2. user space has overidden onNegotiate to reject heartbeats
                    // 3. AMQP server does not want heartbeats
                    // in either case we're no longer going to run further timers.

                    // if we have an initialized connection, user space must have overidden
                    // the onNegotiate method, so we keep using the connection
                    if (self->_connection->initialized()) return;

                    // this is a connection timeout, close the connection with immediate effect
                    return (void) self->_connection->close(true);
                }
                else if (now >= self->_expire)
                {
                    // the server was inactive for a too long period of time,
                    // close the connection with immediate effect
                    self->_timeout = 0;
                    return (void) self->_connection->close(true);
                }
                else if (now >= self->_next)
                {
                    // send the heartbeat
                    self->_connection->heartbeat();

                    // when we should send out the next one
                    self->_next = now + self->_timeout * 500;
                }

                // reschedule the timer
                uv_timer_start(self->_timer, timer_callback, self->_timeout * 500, 0);
            }
        }

    public:

        /**
         *  Constructor
         *  @param  loop                The current event loop
         *  @param  connection          The connection being watched
         *  @param  connection_timeout  The AMQP server connection timeout
         *  @param  fd                  The filedescriptor being watched
         *  @param  events              The events that should be monitored
         */
        Watcher(uv_loop_t *loop,
                TcpConnection *connection,
                uint16_t connection_timeout,
                int fd,
                int events) :
            _connection(connection), _loop(loop), _connection_timeout(connection_timeout)
        {
            // create a new poll
            _poll = new uv_poll_t();
            // create a new timer
            _timer = new uv_timer_t();

            // initialize the libuv handles
            uv_poll_init(_loop, _poll, fd);
            uv_timer_init(loop, _timer);

            // store "this" in the data fields (void*)
            _poll->data = this;
            _timer->data = this;

            // remember current event mask
            _events = events;

            // start the watcher
            uv_poll_start(_poll, events, io_callback);
        }

        /**
         *  Watchers cannot be copied or moved
         *
         *  @param  that    The object to not move or copy
         */
        Watcher(Watcher &&that) = delete;
        Watcher(const Watcher &that) = delete;

        /**
         *  Destructor
         */
        virtual ~Watcher()
        {
            // stop the watcher
            uv_poll_stop(_poll);

            // close the io handle
            ::UV_AUX::close_handle(_poll);

            // stop the timer
            stop_timer();

            // close the timer handle
            ::UV_AUX::close_handle(_timer);
        }

        /**
         *  Set the events for which the filedescriptor is monitored
         *  @param  events  The events to monitor (readable, writable or both)
         */
        void set_event_mask(int events)
        {
            // avoid useless handle update,
            // see https://docs.libuv.org/en/v1.x/poll.html
            if (events != _events)
            {
                // update the events being watched for
                uv_poll_start(_poll, events, io_callback);

                // remember current event mask
                _events = events;
            }
        }

        /**
         *  Apply AMQP server connection timeout watching
         */
        void apply_connection_timeout()
        {
            if (_connection_timeout && !_timeout)
            {
                // schedule the timer
                uv_timer_start(_timer, timer_callback, _connection_timeout * 1000, 0);
            }
        }

        /**
         *  Configure the heartbeat interval to use
         *  @param timeout  The timeout value (seconds, 0 to disable heartbeat monitor.)
         */
        void set_heartbeat(uint16_t timeout)
        {
            _timeout = timeout;
        }

        /**
         *  Schedule the timer
         */
        void set_timer()
        {
            // stop timer in case it was already set
            stop_timer();

            if (_timeout)
            {
                uint64_t now = uv_now(_loop);
                // when we should send out the next heartbeat
                _next = now + _timeout * 500;
                // by when we should expect some server activity
                _expire = _next + _timeout * 1000;

                // schedule the timer
                uv_timer_start(_timer, timer_callback, _timeout * 500, 0);
            }
        }

        /**
         *  Stop the timer
         */
        void stop_timer()
        {
            // do nothing if it was never set
            uv_timer_stop(_timer);
        }
    };

    /**
     *  The event loop
     *  @var uv_loop_t*
     */
    uv_loop_t *_loop;

    /**
     *  All I/O watchers that are active, indexed by their filedescriptor
     *  @var std::map<int,Watcher>
     */
    std::map<int, std::unique_ptr<Watcher>> _watchers;

    /**
     *  AMQP Server connection timeout setting (seconds)
     *  @var uint16_t
     */
    uint16_t _connection_timeout;

    /**
     *  Method that is called by AMQP-CPP to register a filedescriptor for readability or writability
     *  @param  connection  The TCP connection object that is reporting
     *  @param  fd          The filedescriptor to be monitored
     *  @param  flags       Should the object be monitored for readability or writability?
     */
    virtual void monitor(TcpConnection *connection, int fd, int flags) override
    {
        // do we already have this filedescriptor
        auto iter = _watchers.find(fd);

        // was it found?
        if (iter == _watchers.end())
        {
            // a new watcher is required

            // skip if no read/write activity to monitor
            if (flags == 0) return;

            // construct a new watcher, and register as active
            _watchers[fd] =
#if __cplusplus >= 201402L
            // C++14 has make_unique()
                std::make_unique<Watcher>(_loop, connection, _connection_timeout, fd, flags);
#else
                std::unique_ptr<Watcher>(new Watcher(_loop, connection, _connection_timeout, fd, flags));
#endif
            auto &upwatcher = _watchers[fd];

            // apply server connection timeout monitor
            upwatcher->apply_connection_timeout();
        }
        else if (flags == 0)
        {
            // the watcher does not need anymore, unregister
            _watchers.erase(iter);
        }
        else
        {
            // reconfigure the events to monitor
            iter->second->set_event_mask(flags);
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
        iter->second->set_timer();

        // we agree with the timeout
        return timeout;
    }

public:

    /**
     *  Constructor
     *  @param  loop                The event loop to wrap
     *  @param  connection_timeout  The AMQP server connection timeout (seconds)
     */
    LibUvHandler(uv_loop_t *loop, uint16_t connection_timeout = 60)
        : _loop(loop), _connection_timeout(connection_timeout)
    {
        // TODO: make AMQP::readable AMQP::writable (and other flags) constexpr,
        //       then switch to static_assert and get rid of these
        assert(AMQP::readable == UV_READABLE);
        assert(AMQP::writable == UV_WRITABLE);
    }

    /**
     *  Handler cannot be copied or moved
     *
     *  @param  that    The object to not move or copy
     */
    LibUvHandler(LibUvHandler &&that) = delete;
    LibUvHandler(const LibUvHandler &that) = delete;

    /**
     *  Destructor
     */
    virtual ~LibUvHandler() = default;
};

/**
 *  End of namespace
 */
}

