/**
 *  LibBoostAsio.h
 *
 *  Implementation for the AMQP::TcpHandler for boost::asio. You can use this class
 *  instead of a AMQP::TcpHandler class, just pass the boost asio io_context to the
 *  constructor and you're all set.  See examples/libboostasio.cpp for an example.
 *
 *  Watch out: this class was not implemented or reviewed by the original author of
 *  AMQP-CPP. However, we do get a lot of questions and issues from users of this class,
 *  so we cannot guarantee its quality. If you run into such issues too, it might be
 *  better to implement your own handler that interact with boost.
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
    class Watcher : public std::enable_shared_from_this<Watcher>
    {
    private:

        /**
         *  The boost asio io_context which is responsible for detecting events.
         *  @var class boost::asio::io_context&
         */
        boost::asio::io_context & _iocontext;

        using strand_weak_ptr = std::weak_ptr<boost::asio::io_context::strand>;

        /**
         *  The boost asio io_context::strand managed pointer.
         *  @var std::weak_ptr<boost::asio::io_context::strand>
         */
        strand_weak_ptr _wpstrand;

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
         *  @var class boost::asio::steady_timer
         */
        boost::asio::steady_timer _timer;

        /**
         *  Timeout after which the connection is no longer considered alive.
         *  A heartbeat must be sent every _timeout / 2 seconds.
         *  Value zero means heartbeats are disabled, or not yet negotiated.
         *  @var uint16_t
         */
        uint16_t _timeout = 0;

        /**
         *  AMQP Server connection timeout setting (seconds).
         *  @var uint16_t
         */
        uint16_t _connection_timeout = 0;

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
         *  A boolean that indicates if the watcher is monitoring for read events.
         *  @var _read True if reads are being monitored else false.
         */
        bool _read{false};

        /**
         *  A boolean that indicates if the watcher has a pending read event.
         *  @var _read_pending True if read is pending else false.
         */
        bool _read_pending{false};

        /**
         *  A boolean that indicates if the watcher is monitoring for write events.
         *  @var _write True if writes are being monitored else false.
         */
        bool _write{false};

        /**
         *  A boolean that indicates if the watcher has a pending write event.
         *  @var _write_pending True if read is pending else false.
         */
        bool _write_pending{false};

        using handler_cb = std::function<void(boost::system::error_code)>;

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
#if __cplusplus >= 201701L
            // C++17 has weak_from_this()
            std::weak_ptr<Watcher> wpthis = weak_from_this();
#else
            std::weak_ptr<Watcher> wpthis(shared_from_this());
#endif
            const strand_weak_ptr wpstrand = _wpstrand;
            return
#if __cplusplus >= 201402L
                // C++14 lambda has init capture
                [wpthis=std::move(wpthis), wpstrand=std::move(wpstrand), mmfn=std::forward<M>(mmfn),
                                         connection, fd] (const boost::system::error_code& ec)
#else
                [wpthis, wpstrand, mmfn, connection, fd] (const boost::system::error_code& ec)
#endif
                {
                    const std::shared_ptr<Watcher> spwatcher = wpthis.lock();
                    // is the watcher still here?
                    if (!spwatcher) return;

                    const strand_shared_ptr strand = wpstrand.lock();
                    // is the strand still here?
                    if (!strand) return;

                    boost::asio::dispatch(strand->context().get_executor(),
                        std::bind(std::move(mmfn), std::move(spwatcher), ec, connection, fd));
                };
        }

        /**
         *  Handler method that is called by boost's io_context when the socket pumps a read event.
         *  @param  ec          The status of the callback.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         */
        void read_handler(const boost::system::error_code &ec,
                          TcpConnection *const connection,
                          const int fd)
        {
            _read_pending = false;

            if (!ec && _read)
            {
                if (_timeout)
                {
                    // the server has just sent us some data, update the _expire time
                    _expire = std::chrono::steady_clock::now() +
                            std::chrono::seconds((_timeout >> 1) + _timeout + 1);
                }

                connection->process(fd, AMQP::readable);

                _read_pending = true;

                _socket.async_wait(
                    boost::asio::posix::stream_descriptor::wait_read,
                    get_read_handler(connection, fd));
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
         *  @param  ec          The status of the callback.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         */
        void write_handler(const boost::system::error_code ec,
                           TcpConnection *const connection,
                           const int fd)
        {
            _write_pending = false;

            if (!ec && _write)
            {
                connection->process(fd, AMQP::writable);

                _write_pending = true;

                _socket.async_wait(
                    boost::asio::posix::stream_descriptor::wait_write,
                    get_write_handler(connection, fd));
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
         *  @param  ec          The status of the callback.
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descriptor being watched.
         */
        void timer_handler(const boost::system::error_code &ec,
                           TcpConnection *const connection,
                           const int fd)
        {
            if (!ec)
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
         *  @param  wpstrand             A weak pointer to a io_context::strand instance.
         *  @param  fd                   The filedescriptor being watched
         *  @param  connection_timeout   The AMQP server connection timeout
         */
        Watcher(boost::asio::io_context &io_context,
                const strand_weak_ptr wpstrand,
                const int fd,
                uint16_t connection_timeout) :
            _iocontext(io_context),
            _wpstrand(wpstrand),
            _socket(io_context),
            _connection_timeout(connection_timeout),
            _timer(io_context)
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
         *  Destructor
         */
        ~Watcher()
        {
            stop_timer();
            _read = false;
            _write = false;
            _socket.release();
        }

        /**
         *  Change the events for which the filedescriptor is monitored
         *  @param  connection  The connection being watched.
         *  @param  fd          The file descripter being watched.
         *  @param  events
         */
        void events(TcpConnection *const connection, int fd, int events)
        {
            // 1. Handle reads?
            _read = ((events & AMQP::readable) != 0);

            // Read requested but no read pending?
            if (_read && !_read_pending)
            {
                _read_pending = true;

                _socket.async_wait(
                    boost::asio::posix::stream_descriptor::wait_read,
                    get_read_handler(connection, fd));
            }

            // 2. Handle writes?
            _write = ((events & AMQP::writable) != 0);

            // Write requested but no write pending?
            if (_write && !_write_pending)
            {
                _write_pending = true;

                _socket.async_wait(
                    boost::asio::posix::stream_descriptor::wait_write,
                    get_write_handler(connection, fd));
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

    using strand_shared_ptr = std::shared_ptr<boost::asio::io_context::strand>;

    /**
     *  The boost asio io_context::strand managed pointer.
     *  @var std::shared_ptr<boost::asio::io_context::strand>
     */
    strand_shared_ptr _strand;

    /**
     *  Active I/O watchers, indexed by their filedescriptor.
     *  @var std::map<int, Watcher>
     */
    std::map<int, std::shared_ptr<Watcher> > _watchers;

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
    void monitor(TcpConnection *connection, int fd, int flags) override
    {
        // do we already have this filedescriptor
        auto iter = _watchers.find(fd);

        // was it found?
        if (iter == _watchers.end())
        {
            // a new watcher is required
            assert(flags != 0); // a watcher should not be dead on arrival

            // construct a new watcher
            const std::shared_ptr<Watcher> spwatcher =
                std::make_shared<Watcher>(_iocontext, _strand, fd, _connection_timeout);

            // register as active
            _watchers[fd] = spwatcher;

            // apply server connection timeout monitor
            spwatcher->apply_connection_timeout(connection, fd);

            // explicitly set the events to monitor
            spwatcher->events(connection, fd, flags);
        }
        else if (flags == 0)
        {
            // the watcher does not need anymore, unregister
            _watchers.erase(iter);
        }
        else
        {
            // reconfigure the events to monitor
            iter->second->events(connection, fd, flags);
        }
    }

protected:
    /**
     *  Method that is called when the heartbeat timeout is negotiated between the server and the client.
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

public:

    /**
     *  Handler cannot be default constructed.
     *
     *  @param  that    The object to not move or copy
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
        _strand(std::make_shared<boost::asio::io_context::strand>(_iocontext)),
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
    ~LibBoostAsioHandler() override = default;
};


/**
 *  End of namespace
 */
}
