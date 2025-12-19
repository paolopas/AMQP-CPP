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
 *  libUV.cpp
 *
 *  Test program to check AMQP functionality based on libuv.
 *
 *  Compile with (append -lssl if #define USE_AMQPS):
 *  g++ -std=c++17 -Wall -Wno-class-conversion libuv.cpp -o libuv -luv -lamqpcpp -lpthread -ldl
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @author Paolo Pastori
 */

/*
 * uncomment (and link with -lssl) to use amqps instead of plain amqp
 */
//#define USE_AMQPS

/**
 *  Dependencies
 */
#include <uv.h>
#ifdef USE_AMQPS
#include <openssl/ssl.h>
#include <openssl/opensslv.h>
#endif // USE_AMQPS
#include <iomanip>

#include <amqpcpp.h>
#include <amqpcpp/libuv.h>


/**
 *  Class that runs a timer
 */
class MyTimer
{
private:
    /**
     *  The asynchronous timer handle
     *  @var uv_timer_t
     */
    uv_timer_t *_timer;

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
     *  Callback method that is called by libuv when the timer fires
     *  @param  handle  timer handle
     */
    static void callback(uv_timer_t *handle)
    {
        // counter for published messages
        static uint32_t pno = 0;

        // retrieve this
        MyTimer *self = static_cast<MyTimer*>(handle->data);

        // publish a message
        self->_pchannel->publish("", self->_queue, "Hello World");

        std::cout << "message " << ++pno << " published" << std::endl;
    }

public:
    /**
     *  Constructor
     *  @param  loop     The event loop
     *  @param  channel  The channel to use
     *  @param  queue    The name of the queue to use
     */
    MyTimer(uv_loop_t *loop, AMQP::TcpChannel *channel, std::string queue) :
        _pchannel(channel), _queue(queue)
    {
        // create a new timer
        _timer = new uv_timer_t();

        // initialize the timer handle
        uv_timer_init(loop, _timer);

        // store "this" in the data field (void*)
        _timer->data = this;

        // start the timer
        uv_timer_start(_timer, callback, 5, 1005);
    }

    /**
     *  Destructor
     */
    ~MyTimer()
    {
        // stop the timer
        uv_timer_stop(_timer);

        // close the timer handle
        UV_AUX::close_handle(_timer);
    }
};


/**
 *  Custom handler
 */
class MyHandler : public AMQP::LibUvHandler
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
        uv_signal_start(_signal, sig_callabck, SIGINT);

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
     *  The signal handle to be used for process termination
     *  @var uv_signal_t
     */
    uv_signal_t *_signal;

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
     * @param  handle  signal handle
     * @param  signum  signal number
     */
    static void sig_callabck(uv_signal_t *handle, int signum)
    {
        // retrieve this
        MyHandler *self = static_cast<MyHandler*>(handle->data);

        if (self->_conn_to_close) {

            std::cerr << "\nclosing connection...\n";
            // now we gently close the connection
            self->_conn_to_close->close();

            // NOTE: we have to deactivate all handles to leave event loop
            //       otherwise the process will hang
            uv_signal_stop(handle);
            self->stop_timer();
        }
    }

public:

    /**
     *  Constructor
     *  @param  loop  The event loop
     */
    MyHandler(uv_loop_t *loop) : AMQP::LibUvHandler(loop)
    {
        // create a new signal
        _signal = new uv_signal_t();

        // initialize the signal handle
        uv_signal_init(loop, _signal);

        // store "this" in the data field (void*)
        _signal->data = this;
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
        uv_signal_stop(_signal);
        // to deactivate may also call unref as alternative,
        // see https://docs.libuv.org/en/v1.x/handle.html#c.uv_unref
        //uv_unref(reinterpret_cast<uv_handle_t*>(_signal));
    }

    /**
     *  Destructor
     */
    virtual ~MyHandler()
    {
        // stop the signal
        uv_signal_stop(_signal);

        // close the signal handle
        UV_AUX::close_handle(_signal);

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
    auto *loop = uv_default_loop();

    // the handler
    MyHandler handler(loop);

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
      .onSuccess([&connection, &channel, &handler, loop](const std::string &queuename, uint32_t messagecount, uint32_t consumercount) {

        std::cout << "declared queue " << std::quoted(queuename) << std::endl;

/*        // close the channel
        channel.close().onSuccess([&connection, &handler]() {

            std::cout << "channel closed" << std::endl;

            // close the connection
            connection.close();

            // NOTE: we have to deactivate all handles to leave event loop
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

        }).onSuccess([&handler, loop, &channel, queuename](const std::string &tag) {

            // the consumer is ready
            std::cout << "started consuming with tag " << std::quoted(tag) << std::endl;

            // start the publisher too
            handler.set_timer(new MyTimer(loop, &channel, queuename));

        }).onCancelled([&handler](const std::string &tag) {

            // the consumer was cancelled by the server
            std::cout << "consumer " << std::quoted(tag) << " was cancelled" << std::endl;

            // stop the publisher
            handler.stop_timer();

        });
    });

    // run the event loop
    uv_run(loop, UV_RUN_DEFAULT);

    return 0;
}

