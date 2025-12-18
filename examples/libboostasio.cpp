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
 *  libBoostAsio.cpp
 *
 *  Test program to check AMQP-CPP functionality with boost asio.
 *
 *  Compile with (append -lssl if #define USE_AMQPS):
 *  g++ -std=c++17 -Wall -Wno-class-conversion libboostasio.cpp -o boost_test -lboost_system -lamqpcpp -lpthread -ldl
 *
 *  @author Paolo Pastori
 */

/*
 * uncomment (and link with -lssl) to use amqps instead of plain amqp
 */
#define USE_AMQPS

/**
 *  Dependencies
 */
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/io_context.hpp>
#ifdef USE_AMQPS
#include <openssl/ssl.h>
#include <openssl/opensslv.h>
#endif // USE_AMQPS
#include <chrono>
#include <iomanip>

#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>


using boost_errc = boost::system::error_code;

/**
 *  Class that runs a timer
 */
class MyTimer
{
private:
    /**
     *  The actual timer
     *  @var boost::asio::steady_timer
     */
    boost::asio::steady_timer _timer;

    /**
     *  The counter for published messages
     *  @var uint16_t
     */
    uint16_t _pno = 0;

protected:
    /**
     *  Callback method that is called by boost when the timer expires
     *  @param  ec  boost errorcode
     */
    void callback(const boost_errc& ec)
    {
        if (!ec && pchannel)
        {
            // publish a message
            pchannel->publish("", queue, "Hello World");

            std::cout << "message " << ++_pno << " published" << std::endl;

            // reschedule
            start();
        }
    }

public:
    /**
     *  Period of the timer (ms)
     *  @var uint16_t
     */
    uint16_t milli_seconds = 1000;

    /**
     *  Pointer towards the AMQP channel
     *  @var AMQP::TcpChannel
     */
    AMQP::TcpChannel *pchannel;

    /**
     *  Name of the queue
     *  @var std::string
     */
    std::string queue;

    /**
     *  Constructor
     *  @param  ctx       The execution context
     *  @param  pchannel  The TCP channel
     */
    MyTimer(boost::asio::io_context &ctx, AMQP::TcpChannel *pchannel = nullptr) :
        _timer(ctx), pchannel(pchannel)
    {
    }

    /**
     *  Schedule the timer
     */
    void start()
    {
        if (milli_seconds)
        {
            _timer.expires_after(std::chrono::milliseconds(milli_seconds));
            _timer.async_wait([this](const boost_errc& ec) { callback(ec); });
        }
    }

    /**
     *  Stop the timer
     */
    void stop()
    {
        _timer.cancel();
    }

    /**
     *  Destructor
     */
    ~MyTimer()
    {
        stop();
    }
};


/**
 *  Custom handler
 */
class MyHandler : public AMQP::LibBoostAsioHandler
{
private:
    /**
     *  The set of signals to be used for process termination.
     *  @var boost::asio::signal_set
     */
    boost::asio::signal_set _signals;

    /**
     *  The timer for periodical publishing
     *  @var MyTimer*
     */
    MyTimer *_mytimer = nullptr;

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

        // install the signals handler
        _signals.async_wait([this, connection](const boost_errc& ec, int /* signal_number */) {
            if (!ec) {
                // now we gently close the connection
                std::cerr << "\nclosing connection...\n";

                // NOTE: there must be no callabacks queued in execution
                //       context otherwise the process will hang
                if (_mytimer) _mytimer->stop();

                connection->close();
            }
        });
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
    void onBlocked(AMQP::TcpConnection *connection, const char *reason) override
    {
        std::cout << "blocked with reason " << std::quoted(reason) << std::endl;

        // avoid further publishing since we are blocked
        if (_mytimer) _mytimer->stop();

        // NOTE: doing a connection->close() has no effect here as
        //       the server has already end to listen to us,
        //       in this case if you want to leave without incur in
        //       a deadlock must call the release mehod of the handler
        // NOTE: there must also be no callabacks queued in execution
        //       context otherwise the process will hang (timers, signals...)

        // always tell the handler as good practice
        return LibBoostAsioHandler::onBlocked(connection, reason);
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

protected:
    /**
     *  Method that is called when the heartbeat frequency is negotiated
     *
     *  @param  connection  The TCP connection
     *  @param  interval    The suggested interval from the server
     *  @return uint16_t    The interval to use
     */
    uint16_t onNegotiate(AMQP::TcpConnection *connection, uint16_t interval) override
    {
        // could override server proposed interval here, i.e.
        // uncoment to force interval 0 to suppress heartbeat
        //interval = 0;

        // must tell the handler
        return LibBoostAsioHandler::onNegotiate(connection, interval);
    }

public:
    /**
     *  Constructor
     *  @param  ctx  The execution context
     */
    MyHandler(boost::asio::io_context &ctx)
        : AMQP::LibBoostAsioHandler(ctx), _signals(ctx, SIGINT, SIGTERM)
    {
    }

    /**
     *  Install the user timer
     */
    void set_timer(MyTimer *timer)
    {
        _mytimer = timer;
    }

    /**
     *  Stop listening for signals
     */
    void stop_signals()
    {
        _signals.cancel();
    }

    /**
     *  Destructor
     */
    virtual ~MyHandler() = default;
};


/**
 *  Access to execution context
 */
boost::asio::io_context &get_context()
{
    static boost::asio::io_context ctx;

    return ctx;
}

/**
 *  Main program
 *  @return int
 */
int main()
{
    // handler for libboostasio
    MyHandler handler(get_context());

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

    // construct a timer that is going to publish stuff
    MyTimer timer(get_context(), &channel);
    // and install it on the handler
    handler.set_timer(&timer);

    // create a temporary queue
    channel.declareQueue(AMQP::exclusive)
      .onSuccess([&connection, &channel, &timer, &handler](const std::string &queuename, uint32_t messagecount, uint32_t consumercount) {

        std::cout << "declared queue " << quoted(queuename) << std::endl;

        timer.queue = queuename;

/*        // close the channel
        channel.close().onSuccess([&connection, &channel, &handler]() {

            std::cout << "channel closed" << std::endl;

            // close the connection
            connection.close();

            // NOTE: there must be no callabacks queued in execution
            //       context otherwise the process will hang
            handler.stop_signals();

        }); */

        // start a consumer
        channel.consume(queuename).onReceived([&channel, queuename](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered) {

            std::cout << "received " << deliveryTag << std::endl;

/*            // we remove the queue, to see if this indeed triggers the onCancelled method
            if (deliveryTag > 4) channel.removeQueue(queuename); */

            // ack the message
            channel.ack(deliveryTag);

        }).onSuccess([&timer](const std::string &tag) {

            // the consumer is ready
            std::cout << "started consuming with tag " << quoted(tag) << std::endl;

            // start the publisher too
            timer.start();

        }).onCancelled([&timer](const std::string &tag) {

            // the consumer was cancelled by the server
            std::cout << "consumer " << quoted(tag) << " was cancelled" << std::endl;

            // stop the publisher
            timer.stop();

        }).onError([&timer, &connection, &handler](const char *message) {

            // the consumer was cancelled by the server
            std::cout << "consumer operation failed" << std::endl;

            // stop the publisher
            timer.stop();

            // close the connection
            connection.close();

            // NOTE: there must be no callabacks queued in execution
            //       context otherwise the process will hang
            handler.stop_signals();

        });

/*        // close the connection
        connection.close();
        // NOTE: this will have no effect, as the timer is not started yet
        timer.stop(); */
    });

    // run the handler
    return get_context().run();
}
