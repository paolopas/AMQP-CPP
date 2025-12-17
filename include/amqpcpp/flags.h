/**
 *  AmqpFlags.h
 *
 *  The various flags that are supported
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  All bit flags
 *  @var int
 */
constexpr int durable      = 0x1;
constexpr int autodelete   = 0x2;
constexpr int active       = 0x4;
constexpr int passive      = 0x8;
constexpr int ifunused     = 0x10;
constexpr int ifempty      = 0x20;
constexpr int global       = 0x40;
constexpr int nolocal      = 0x80;
constexpr int noack        = 0x100;
constexpr int exclusive    = 0x200;
constexpr int nowait       = 0x400;
constexpr int mandatory    = 0x800;
constexpr int immediate    = 0x1000;
constexpr int redelivered  = 0x2000;
constexpr int multiple     = 0x4000;
constexpr int requeue      = 0x8000;
constexpr int internal     = 0x10000;

/**
 *  Flags for event loops
 */
constexpr int readable     = 0x1;
constexpr int writable     = 0x2;

/**
 *  End of namespace
 */
}

