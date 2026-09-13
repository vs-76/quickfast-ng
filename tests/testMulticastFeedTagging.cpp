// Copyright (c) 2026, QuickFAST contributors.
// All rights reserved.
// See the file license.txt for licensing information.
//
// MulticastFeedHost::feedReceived(error, buffer, bytesReceived) carries no
// parameter naming which feed produced the packet, so a host servicing more
// than one feed (Feed A / Feed B redundancy) has no way to tell them apart
// once the packet has arrived. MulticastFeedBase::handleReceive now tags the
// accepted buffer with a pointer to its own name_ via LinkedBuffer::setExtra,
// and MulticastReceiverBase::feedReceived is protected (rather than private)
// so a derived receiver can read that tag before the buffer is queued.
//
// This exercises the real asynchronous receive path end to end -- a genuine
// UDP datagram is sent to the feed's bound port and decoded off the wire --
// rather than calling any private method directly. Skips rather than fails
// when the host has no usable multicast join, matching the caution already
// established by testConfiguredSourcesSelectTheSourceSpecificReceiver in
// testSourceSpecificMulticast.cpp.
#include <Common/QuickFASTPch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include <Codecs/TemplateRegistry.h>
#include <Common/Logger.h>
#include <Communication/MulticastReceiver.h>
#include <asio.hpp>

using namespace QuickFAST;

namespace
{
  /// @brief A logger that says nothing, because nothing is expected.
  class SilentLogger : public Common::Logger
  {
  public:
    virtual bool wantLog(unsigned short)
    {
      return false;
    }
    virtual bool logMessage(unsigned short, const std::string &)
    {
      return true;
    }
    virtual bool reportDecodingError(const std::string &)
    {
      return true;
    }
    virtual bool reportCommunicationError(const std::string &)
    {
      return true;
    }
  };

  /// @brief Counts buffers rather than decoding them.
  class CountingAssembler : public Communication::Assembler
  {
  public:
    CountingAssembler(Common::Logger & logger)
      : Communication::Assembler(
          Codecs::TemplateRegistryPtr(new Codecs::TemplateRegistry), logger)
    {
    }

    virtual void receiverStarted(Communication::Receiver &) {}
    virtual void receiverStopped(Communication::Receiver &) {}

    virtual bool serviceQueue(Communication::Receiver & receiver)
    {
      Communication::LinkedBuffer * buffer = receiver.getBuffer(false);
      while(buffer != 0)
      {
        receiver.releaseBuffer(buffer);
        buffer = receiver.getBuffer(false);
      }
      return true;
    }
  };

  /// @brief A single-feed receiver that records what the feed tagged the
  /// buffer with, before forwarding to the real completion handling.
  class TaggingReceiver : public Communication::MulticastReceiver
  {
  public:
    TaggingReceiver(
      const std::string & multicastGroupIP,
      const std::string & listenInterfaceIP,
      const std::string & bindIP,
      unsigned short portNumber)
      : Communication::MulticastReceiver(
          multicastGroupIP, listenInterfaceIP, bindIP, portNumber)
    {
    }

    std::atomic<const void *> lastExtra{nullptr};
    std::atomic<size_t> tagged{0};

  protected:
    // MulticastReceiverBase::feedReceived is protected (not private) exactly
    // so this override is possible -- the change under test.
    virtual void feedReceived(
      const asio::error_code & error,
      Communication::LinkedBuffer * buffer,
      size_t bytesReceived) override
    {
      if(!error && buffer != 0 && bytesReceived > 0)
      {
        lastExtra.store(buffer->extra());
        ++tagged;
      }
      handleReceive(error, buffer, bytesReceived);
    }
  };
}

TEST(QuickFAST, testAnySourceFeedTagsBufferWithItsOwnName)
{
  // "default" is the name AnySourceMulticastFeed is given by
  // MulticastReceiver's convenience constructor.
  TaggingReceiver receiver("239.7.8.9", "0.0.0.0", "0.0.0.0", 23990);

  SilentLogger logger;
  CountingAssembler assembler(logger);
  if(!receiver.start(assembler, 1500, 4))
  {
    GTEST_SKIP() << "multicast group join unavailable in this environment";
  }
  receiver.runThreads(1, false);

  // A plain unicast datagram to the feed's bound port is enough: the socket
  // was bound with bindIP "0.0.0.0" (the group address' own wildcard), so it
  // accepts any packet addressed to that local port, not only ones that
  // arrived via multicast delivery.
  {
    asio::io_context sendIo;
    asio::ip::udp::socket sender(sendIo, asio::ip::udp::v4());
    const std::string payload("QuickFAST feed tagging test payload");
    sender.send_to(
      asio::buffer(payload),
      asio::ip::udp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 23990));
  }

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while(receiver.tagged.load() == 0 && std::chrono::steady_clock::now() < deadline)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  receiver.stop();
  receiver.joinThreads();

  ASSERT_GT(receiver.tagged.load(), 0u)
    << "no packet arrived; environment may not deliver loopback UDP";
  const void * const extra = receiver.lastExtra.load();
  ASSERT_NE(extra, static_cast<const void *>(0));
  const std::string & name = *static_cast<const std::string *>(extra);
  EXPECT_EQ(name, "default");
}
