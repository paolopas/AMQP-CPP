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
 *  LibEvent.h
 *
 *  Implementation for the AMQP::TcpHandler that is optimized for libevent. You can
 *  use this class instead of a AMQP::TcpHandler class, just pass the event loop
 *  to the constructor and you're all set.  See examples/libevent.cpp for an example.
 *
 *
 *  @author Brent Dimmig <brentdimmig@gmail.com>
 *  @author Paolo Pastori
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include <event2/event.h>
#if __cplusplus >= 201402L
// >= C++14, using make_unique
#include <memory>
#endif
#include <map>
#include <cassert>

#include "amqpcpp/linux_tcp.h"


// TODO: maybe for AMQP-CPP VERSION > 4.3.27 we can use
//       these in place of the assert placed into ctor
//static_assert(AMQP::readable == (EV_READ >> 1), "libevent vs AMQP-CPP flags mismatch");
//static_assert(AMQP::writable == (EV_WRITE >> 1), "libevent vs AMQP-CPP flags mismatch");

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class LibEventHandler : public TcpHandler
{
private:
    /**
     *  Helper class that wraps a libevent I/O watcher
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
         *  @var struct event_base
         */
        struct event_base *_base;

        /**
         *  The io event structure
         *  @var struct event
         */
        struct event *_io;

        /**
         *  The timer event structure
         *  @var struct event
         */
        struct event *_timer;

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
         *  @var struct timeval
         */
        struct timeval _next;

        /**
         *  When does the connection expire / was the server idle for too long?
         *  @var struct timeval
         */
        struct timeval _expire;

        /**
         *  The events for which the filedescriptor is actually monitored.
         *  @var int
         */
        int _events = 0;

        /**
         *  Helper method to check for connection socket status
         *  @return bool
         */
        bool is_open()
        {
            return (_connection->fileno() != -1);
        }

        /**
         *  Callback method that is called by libevent when a filedescriptor becomes active
         *  @param  fd      The filedescriptor with an event
         *  @param  what    Events triggered
         *  @param  arg     (void *) this
         */
        static void io_callback(int fd, short what, void *arg)
        {
            Watcher* self = static_cast<Watcher*>(arg);

            // is the socket still open?
            if (self->is_open())
            {
                struct timeval now, delta;
                event_base_gettimeofday_cached(self->_base, &now);

                // setup flags
                int events = (what >> 1) & (AMQP::writable | AMQP::readable);

                if (events & AMQP::readable)
                {
                    // the server is sending data, update the _expire time
                    delta = { self->_timeout + (self->_timeout >> 1), 500'000 };
                    evutil_timeradd(&now, &delta, &self->_expire);
                }
                if (events & AMQP::writable)
                {
                    // the client is sending data, update the _next time
                    delta = { self->_timeout >> 1, 500'000 };
                    evutil_timeradd(&now, &delta, &self->_next);
                }

                self->_connection->process(fd, events);
            }
        }

        /**
         *  Callback method that is called by libevent when a timeout elapses
         *  @param  fd      The filedescriptor associated
         *  @param  what    Events triggered (always EV_TIMEOUT)
         *  @param  arg     (void *) this
         */
        static void timer_callback(int fd, short what, void *arg)
        {
            Watcher* self = static_cast<Watcher*>(arg);

            // is the socket still open?
            if (self->is_open())
            {
                struct timeval now;
                event_base_gettimeofday_cached(self->_base, &now);

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
                else if (evutil_timercmp(&now, &self->_expire, >=))
                {
                    // the server was inactive for a too long period of time,
                    // close the connection with immediate effect
                    self->_timeout = 0;
                    return (void) self->_connection->close(true);
                }
                else if (evutil_timercmp(&now, &self->_next, >=))
                {
                    // send the heartbeat
                    self->_connection->heartbeat();

                    // when we should send out the next one
                    struct timeval delta = { self->_timeout >> 1, 500'000 };
                    evutil_timeradd(&now, &delta, &self->_next);
                }
            }
        }

    public:

        /**
         *  Constructor
         *  @param  base                The current event loop
         *  @param  connection          The connection being watched
         *  @param  connection_timeout  The AMQP server connection timeout
         *  @param  fd                  The filedescriptor being watched
         *  @param  events              The events that should be monitored
         */
        Watcher(struct event_base *base,
                TcpConnection *connection,
                uint16_t connection_timeout,
                int fd,
                int events) :
            _connection(connection), _base(base), _connection_timeout(connection_timeout)
        {
            // initialize the io event
            short event_flags = EV_PERSIST;
            if (events & AMQP::readable) event_flags |= EV_READ;
            if (events & AMQP::writable) event_flags |= EV_WRITE;

            _io = event_new(base, fd, event_flags, io_callback, this);

            // makes it pending (start)
            event_add(_io, nullptr);

            // remember current event mask
            _events = events;

            // initialize the timer event
            _timer = event_new(base, fd, EV_PERSIST, timer_callback, this);
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
            // deallocate the io event
            event_free(_io);
            // deallocate the timer event
            event_free(_timer);
        }

        /**
         *  Set the events for which the filedescriptor is monitored
         *  @param  events  The events to monitor (readable, writable or both)
         */
        void set_event_mask(int events)
        {
            if (events != _events)
            {
                // make the event non pending (stop)
                event_del(_io);

                // setup libevent flags
                short event_flags = EV_PERSIST | static_cast<short>(events << 1);

                // set the events
                event_assign(_io, _base, event_get_fd(_io), event_flags,
                             io_callback, this);

                // makes it pending (start)
                event_add(_io, nullptr);

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
                // makes the timer pending (start)
                struct timeval delta = { _connection_timeout, 0 };
                event_add(_timer, &delta);
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
                struct timeval now;
                event_base_gettimeofday_cached(_base, &now);

                // when we should send out the next one
                struct timeval delta = { _timeout >> 1, 500'000 };
                evutil_timeradd(&now, &delta, &_next);

                // the server is sending data, update the _expire time
                struct timeval deltb = { _timeout, 0 };
                evutil_timeradd(&_next, &deltb, &_expire);

                // makes the timer pending (start)
                event_add(_timer, &delta);
            }
        }

        /**
         *  Stop the timer
         */
        void stop_timer()
        {
            // make the event non pending (stop)
            // do nothing if it was never set
            event_del(_timer);
        }
    };

    /**
     *  The event loop
     *  @var struct event_base*
     */
    struct event_base *_base;

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
                std::make_unique<Watcher>(_base, connection, _connection_timeout, fd, flags);
#else
                std::unique_ptr<Watcher>(new Watcher(_base, connection, _connection_timeout, fd, flags));
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
     *  @param  timeout         The suggested timeout from the server
     *  @return uint16_t        The timeout to use
     */
    virtual uint16_t onNegotiate(TcpConnection *connection, uint16_t timeout) override
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
     *  @param  base                The event loop to wrap
     *  @param  connection_timeout  The AMQP server connection timeout (seconds)
     */
    LibEventHandler(struct event_base *base, uint16_t connection_timeout = 60)
        : _base(base), _connection_timeout(connection_timeout)
    {
        // TODO: make AMQP::readable AMQP::writable (and other flags) constexpr,
        //       then switch to static_assert and get rid of these
        assert(AMQP::readable == (EV_READ >> 1));
        assert(AMQP::writable == (EV_WRITE >> 1));
    }

    /**
     *  Handler cannot be copied or moved
     *
     *  @param  that    The object to not move or copy
     */
    LibEventHandler(LibEventHandler &&that) = delete;
    LibEventHandler(const LibEventHandler &that) = delete;

    /**
     *  Destructor
     */
    virtual ~LibEventHandler() = default;
};

/**
 *  End of namespace
 */
}

