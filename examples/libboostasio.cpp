/**
 *  LibBoostAsio.cpp
 * 
 *  Test program to check AMQP functionality based on Boost's asio io_service.
 * 
 *  @author Gavin Smith <gavin.smith@coralbay.tv>
 *
 *  Compile with
 *  g++ -std=c++17 libboostasio.cpp -o boost_test -lpthread -ldl -lboost_system -lamqpcpp
 */

/**
 *  Dependencies
 */
#include <boost/asio/io_context.hpp>


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

    // handler for libboostasio
    AMQP::LibBoostAsioHandler handler(context);

    // make a connection
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://guest:guest@localhost/"));

    // we need a channel too
    AMQP::TcpChannel channel(&connection);

    channel.onReady([]() {
        std::cout << "channel ready" << std::endl;
            });
    channel.onError([](const char *message) {
        std::cout << "channel error: " << message << std::endl;
            });

    // create a temporary queue
    channel.declareQueue(AMQP::exclusive).onSuccess([&connection](const std::string &name, uint32_t messagecount, uint32_t consumercount) {

        // report the name of the temporary queue
        std::cout << "declared queue " << name << std::endl;

        // now we can close the connection
        connection.close();

    }).onError([](const char *message) {

        // something went wrong creating the queue (or even before)
        std::cerr << "declareQueue error: " << message << std::endl;
    });

    // run the handler
    // at the moment, one will need SIGINT to stop.  In time, should add signal handling through boost API.
    return context.run();
}

