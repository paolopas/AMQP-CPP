/**
 *  LibBoostAsio.cpp
 *
 *  Test program to check AMQP functionality based on Boost's asio io_service.
 *
 *  @author Gavin Smith <gavin.smith@coralbay.tv>
 *
 *  Compile with:
 *  g++ -std=c++17 -Wall  libboostasio.cpp -o boost_test  -lboost_system -lamqpcpp -lpthread -ldl
 */

/**
 *  Dependencies
 */
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>

/**
 *  Main program
 *  @return int
 */
int main()
{
    // access to the boost asio handler
    boost::asio::io_context context;

    // set of signal for process termination (CTRL-C, kill)
    boost::asio::signal_set signals(context, SIGINT, SIGTERM);

    // handler for libboostasio
    AMQP::LibBoostAsioHandler handler(context);

    // make a connection
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://guest:guest@localhost/"));

    // we need a channel too
    AMQP::TcpChannel channel(&connection);

    channel.onReady([&signals, &connection]() {
        std::cout << "hit CTRL-C to exit\n\nchannel ready\n";

        // install the signals handler
        signals.async_wait([&connection](const boost::system::error_code& ec, int /* signal_number */)
        {
            if (!ec) {
                // now we gently close the connection
                std::cerr << "\nclosing connection...\n";
                connection.close();
            }
        });
    });
    channel.onError([](const char *message) {
        std::cerr << "channel error: " << message << std::endl;
    });

    // create a temporary queue
    channel.declareQueue(AMQP::exclusive
     ).onSuccess([](const std::string &name, uint32_t /* messagecount */, uint32_t /* consumercount */) {

        // report the name of the temporary queue
        std::cout << "declared queue " << name << std::endl;

    }).onError([](const char *message) {

        // something went wrong creating the queue (or even before)
        std::cerr << "declareQueue error: " << message << std::endl;
    });

    // run the handler
    return context.run();
}
