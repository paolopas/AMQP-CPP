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
 *  LibEvent.cpp
 *
 *  Test program to check AMQP functionality based on libevent.
 *
 *  Compile with (append -lssl if #define USE_AMQPS):
 *  g++ -std=c++17 -Wall -Wno-class-conversion libevent.cpp -o libevent -levent -lamqpcpp -lpthread -ldl
 *
 *  @author Brent Dimmig <brentdimmig@gmail.com>
 *  @author Paolo Pastori
 */

/*
 * uncomment (and link with -lssl) to use amqps instead of plain amqp
 */
//#define USE_AMQPS

/**
 *  Dependencies
 */
#include <signal.h>
#include <event2/event.h>
#ifdef USE_AMQPS
#include <openssl/ssl.h>
#include <openssl/opensslv.h>
#endif // USE_AMQPS
#include <iomanip>

#include <amqpcpp.h>
#include <amqpcpp/libevent.h>


/**
 *  Class that runs a timer
 */
class MyTimer
{
private:
    /**
     *  The timer event structure
     *  @var struct event
     */
    struct event *_timer;

    /**
     *  The AMQP channel
     *  @var AMQP::TcpChannel
     */
    AMQP::TcpChannel *_pchannel;

    /**
     *  Name of the queue
     *  @var std::string
     */
    std::string _queue;

    /**
     *  Callback method that is called by libevent when a timeout elapses
     *  @param  fd      The filedescriptor associated (usually -1)
     *  @param  what    Events triggered (always EV_TIMEOUT)
     *  @param  arg     (void *) this
     */
    static void callback(int fd, short what, void *arg)
    {
        // counter for published messages
        static uint32_t pno = 0;

        // retrieve this
        MyTimer *self = static_cast<MyTimer*>(arg);

        // reschedule the timer with a new timeout
        struct timeval tmout = { 1, 0 };   // periodical delay
        event_add(self->_timer, &tmout);

        // publish a message
        self->_pchannel->publish("", self->_queue, "Hello World");

        std::cout << "message " << ++pno << " published" << std::endl;
    }

public:
    /**
     *  Constructor
     *  @param  base     The event loop
     *  @param  channel  The channel to use
     *  @param  queue    The name of the queue to use
     */
    MyTimer(struct event_base *base, AMQP::TcpChannel *channel, std::string queue) :
        _pchannel(channel), _queue(queue)
    {
        // initialize the timer event
        _timer = event_new(base, -1, EV_PERSIST, callback, this);

        // makes the timer pending (start)
        struct timeval tmout = { 5, 0 };   // intial delay
        event_add(_timer, &tmout);
    }

    /**
     *  Destructor
     */
    ~MyTimer()
    {
        // deallocate the timer event
        event_free(_timer);
    }
};


/**
 *  Custom handler
 */
class MyHandler : public AMQP::LibEventHandler
{
private:
    /**
     *  Method that is called when a connection error occurs
     *  @param  connection  The TCP connection
     *  @param  message     The error message
     */
    void onError(AMQP::TcpConnection* /* connection */, const char *message) override
    {
        std::cout << "error: " << std::quoted(message) << std::endl;
    }

    /**
     *  Method that is called when the TCP connection ends up in a connected state
     *  @param  connection  The TCP connection
     */
    void onConnected(AMQP::TcpConnection *connection) override
    {
        std::cout << "connected\n";

        // save the connection to close and start the signal
        _conn_to_close = connection;
        event_add(_signal, nullptr);

        std::cout << "\n  Hit CTRL-C to exit\n" << std::endl;
    }

    /**
     *  Method that is called when the TCP connection ends up in a ready
     *  @param  connection  The TCP connection
     */
    void onReady(AMQP::TcpConnection* /* connection */) override
    {
        std::cout << "ready" << std::endl;
    }

    /**
     *  Method that is called when the TCP connection is closed
     *  @param  connection  The TCP connection
     */
    void onClosed(AMQP::TcpConnection* /* connection */) override
    {
        std::cout << "closed" << std::endl;
    }

    /**
     *  Method that is called when the TCP connection was blocked
     *  @param  connection      The connection that was blocked
     *  @param  reason          Why was the connection blocked
     */
    void onBlocked(AMQP::TcpConnection* /* connection */, const char *reason) override
    {
        std::cout << "blocked with reason " << std::quoted(reason) << std::endl;
    }

    /**
     *  Method that is called when the TCP connection is no longer blocked
     *  @param  connection      The connection that is no longer blocked
     */
    void onUnblocked(AMQP::TcpConnection* /* connection */) override
    {
        std::cout << "unblocked" << std::endl;
    }

    /**
     *  Method that is called when the TCP connection is lost or closed
     *  @param  connection  The TCP connection
     */
    void onLost(AMQP::TcpConnection* /* connection */) override
    {
        std::cout << "lost" << std::endl;
    }

    /**
     *  Method that is called when the TCP connection is detached
     *  @param  connection  The TCP connection
     */
    void onDetached(AMQP::TcpConnection* /* connection */) override
    {
        std::cout << "detached" << std::endl;
    }


    /**
     *  The signal event structure to be used for process termination
     *  @var uv_signal_t
     */
    struct event *_signal;

    /**
     *  The connection to be closed when pressing CTRL-C
     *  @var TcpConnection
     */
    AMQP::TcpConnection *_conn_to_close = nullptr;

    /**
     *  The timer used for periodical publishing
     *  @var MyTimer
     */
    MyTimer *_mytimer = nullptr;

    /**
     * Callback method that is called by libuv when a signal is catched
     * @param  signo   The catched signal
     * @param  what    Events triggered (always EV_SIGNAL)
     * @param  arg     (void *) this
     */
    static void sig_callabck(int signo, short what, void *arg)
    {
        // retrieve this
        MyHandler *self = static_cast<MyHandler*>(arg);

        if (self->_conn_to_close) {

            std::cerr << "\nclosing connection...\n";
            // now we gently close the connection
            self->_conn_to_close->close();

            // NOTE: we have to empty the event loop before leave
            //       otherwise the process will hang
            event_del(self->_signal);
            self->stop_timer();
        }
    }

public:

    /**
     *  Constructor
     *  @param  base  The event loop
     */
    MyHandler(struct event_base *base) : AMQP::LibEventHandler(base)
    {
        // initialize the signal event
        _signal = evsignal_new(base, SIGINT, sig_callabck, this);
    }

    /**
     *  Install the user timer
     */
    void set_timer(MyTimer *timer)
    {
        _mytimer = timer;
    }

    /**
     *  Stop the timer
     */
    void stop_timer()
    {
        if (_mytimer)
        {
            delete _mytimer;
            _mytimer = nullptr;
        }
    }

    /**
     *  Stop listening for signals
     */
    void stop_signals()
    {
        event_del(_signal);
    }

    /**
     *  Destructor
     */
    virtual ~MyHandler()
    {
        // deallocate the signal event
        event_free(_signal);

        stop_timer();
    }
};


/**
 *  Main program
 *  @return int
 */
int main()
{
    // the event loop
    auto *base = event_base_new();

    // the handler
    MyHandler handler(base);

#ifdef USE_AMQPS
    // init the SSL library
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    SSL_library_init();
#else
    OPENSSL_init_ssl(0, NULL);
#endif

    AMQP::Address address("amqps://guest:guest@localhost/");
#else
    AMQP::Address address("amqp://guest:guest@localhost/");
#endif // USE_AMQPS

    // make a connection
    AMQP::TcpConnection connection(&handler, address);

    // make a channel
    AMQP::TcpChannel channel(&connection);

    // create a temporary queue
    channel.declareQueue(AMQP::exclusive)
      .onSuccess([&connection, &channel, &handler, base](const std::string &queuename, uint32_t messagecount, uint32_t consumercount) {

        std::cout << "declared queue " << std::quoted(queuename) << std::endl;

/*        // close the channel
        channel.close().onSuccess([&connection, &handler]() {

            std::cout << "channel closed" << std::endl;

            // close the connection
            connection.close();

            // NOTE: we have to empty the event loop before leave
            //       otherwise the process will hang
            handler.stop_signals();

        }); */

        // start a consumer
        channel.consume(queuename).onReceived([&channel, queuename](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {

            std::cout << "received " << deliveryTag << std::endl;

/*            // we remove the queue, to see if this indeed triggers the onCancelled method
            if (deliveryTag > 4) channel.removeQueue(queuename); */

            // ack the message
            channel.ack(deliveryTag);

        }).onSuccess([&handler, base, &channel, queuename](const std::string &tag) {

            // the consumer is ready
            std::cout << "started consuming with tag " << std::quoted(tag) << std::endl;

            // start the publisher too
            handler.set_timer(new MyTimer(base, &channel, queuename));

        }).onCancelled([&handler](const std::string &tag) {

            // the consumer was cancelled by the server
            std::cout << "consumer " << std::quoted(tag) << " was cancelled" << std::endl;

            // stop the publisher
            handler.stop_timer();

        });
    });

    // run the event loop
    event_base_dispatch(base);

    // deallocate the loop
    event_base_free(base);

    return 0;
}

