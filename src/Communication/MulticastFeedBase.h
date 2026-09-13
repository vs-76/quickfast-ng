// Copyright (c) 2026, QuickFAST contributors.
// All rights reserved.
// See the file license.txt for licensing information.
//
#ifdef _MSC_VER
# pragma once
#endif
#ifndef MULTICASTFEEDBASE_H
#define MULTICASTFEEDBASE_H
// All inline, do not export.
//#include <Common/QuickFAST_Export.h>
#include <Communication/AsioService.h>
#include <Communication/LinkedBuffer.h>
#include <Communication/Receiver.h>

namespace QuickFAST
{
  namespace Communication
  {
    /// @brief What a multicast feed needs from the receiver that owns it.
    ///
    /// A feed reads into buffers the receiver owns and must hand them back
    /// however the read turned out.  This interface is the whole of that
    /// conversation, which keeps the feed independent of which receiver
    /// created it: any-source and source-specific receivers share one feed
    /// implementation and differ only in how they join a group.
    class MulticastFeedHost
    {
    public:
      virtual ~MulticastFeedHost()
      {
      }

      /// @brief Accept a completed read from one of the feeds.
      ///
      /// The receiver counts the packet, queues or recycles the buffer, and
      /// starts the next read.  A bytesReceived of zero recycles the buffer
      /// without queueing it.
      ///
      /// @param error indicates status of the receive
      /// @param buffer into which the receive happened
      /// @param bytesReceived how much data is in the buffer
      virtual void feedReceived(
        const asio::error_code & error,
        LinkedBuffer * buffer,
        size_t bytesReceived) = 0;

      /// @brief Is the receiver shutting down?
      /// @returns true once stop() has been called on the receiver.
      virtual bool feedStopping() const = 0;
    };

    /// @brief One multicast group subscription feeding a receiver.
    ///
    /// Everything here is independent of the flavour of multicast in use:
    /// opening and binding the socket, the asynchronous read loop, and the
    /// join/leave points in the start, stop, pause, and resume paths.  A
    /// derived class supplies only join() and leave(), and may narrow which
    /// senders are acceptable by overriding acceptSender().
    ///
    /// @par Example
    /// @code
    /// class AnySourceMulticastFeed : public MulticastFeedBase
    /// {
    ///   // ... constructor forwards to MulticastFeedBase ...
    ///   void join() { socket().set_option(joinRequest); }
    ///   void leave() { socket().set_option(leaveRequest); }
    /// };
    /// @endcode
    class MulticastFeedBase
    {
    public:
      /// @brief Construct a feed.
      /// @param host is the receiver that owns this feed
      /// @param ioService services the asynchronous reads
      /// @param name identifies this feed in display/log messages
      /// @param multicastGroupIP multicast address as a text string
      /// @param listenInterfaceIP identifies the network interface to be used.
      ///        0.0.0.0 means "let the system choose"
      /// @param bindIP the IP to be used to bind the socket
      /// @param portNumber port number
      MulticastFeedBase(
        MulticastFeedHost & host,
        AsioService & ioService,
        const std::string & name,
        const std::string & multicastGroupIP,
        const std::string & listenInterfaceIP,
        const std::string & bindIP,
        unsigned short portNumber
        )
        : host_(host)
        , name_(name)
        , listenInterface_(asio::ip::make_address(listenInterfaceIP))
        , portNumber_(portNumber)
        , multicastGroup_(asio::ip::make_address(multicastGroupIP))
        , bindAddress_(asio::ip::make_address(bindIP))
        , endpoint_(listenInterface_, portNumber)
        , socket_(ioService)
        , joined_(false)
        , readInProgress_(false)
      {
      }

      virtual ~MulticastFeedBase()
      {
      }

      /// @brief The name used to identify this feed in messages.
      const std::string & name()const
      {
        return name_;
      }

      /// @brief Is this feed idle enough to start another read?
      bool canStartRead() const
      {
        return !readInProgress_;
      }

      /// @brief Open and bind the socket, then join the group.
      ///
      /// Throws if the socket cannot be opened or bound, or if the join is
      /// refused.  Nothing is left joined when it throws.
      void initializeReceiver()
      {
        socket_.open(endpoint_.protocol());
        // PVS-Studio models the throwing overload of the preceding Asio call
        // as never returning, so it treats the rest of the function as
        // unreachable.
        socket_.set_option(asio::ip::udp::socket::reuse_address(true)); //-V779
        asio::ip::udp::endpoint bindpoint(bindAddress_, portNumber_);
        socket_.bind(bindpoint);

        join();
        joined_ = true;
      }

      /// @brief Start an asynchronous read into the buffer.
      /// @param buffer to be filled
      /// @returns true if a read was started
      bool fillBuffer(LinkedBuffer * buffer, std::unique_lock<std::mutex> &)
      {
        if(readInProgress_)
        {
          return false;
        }
        readInProgress_ = true;
        socket_.async_receive_from(
          asio::buffer(buffer->get(), buffer->capacity()),
          senderEndpoint_,
          [this, buffer](const asio::error_code& error, std::size_t bytes_transferred)
          {
            this->handleReceive(error, buffer, bytes_transferred);
          });
        return true;
      }

      /// @brief Attempt to cancel any receive request in progress.
      void stop()
      {
        asio::error_code ec;
        socket_.cancel(ec);
      }

      /// @brief Temporarily leave the group.
      void pause()
      {
        if(joined_)
        {
          leave();
          joined_ = false;
        }
      }

      /// @brief Rejoin the group after pause().
      void resume()
      {
        if(!joined_)
        {
          join();
          joined_ = true;
        }
      }

      /// @brief The network interface on which this feed subscribes.
      asio::ip::address listenInterface()const
      {
        return listenInterface_;
      }

      /// @brief The port this feed listens on.
      unsigned short portNumber()const
      {
        return portNumber_;
      }

      /// @brief The multicast group this feed subscribes to.
      asio::ip::address multicastGroup()const
      {
        return multicastGroup_;
      }

      /// @brief The address to which the socket is bound.
      asio::ip::address bindAddress()const
      {
        return bindAddress_;
      }

      /// @brief The interface endpoint described by this feed.
      asio::ip::udp::endpoint endpoint()const
      {
        return endpoint_;
      }

      /// @brief Where the most recent packet came from.
      asio::ip::udp::endpoint senderEndpoint()const
      {
        return senderEndpoint_;
      }

      /// @brief The socket carrying this feed.
      asio::ip::udp::socket & socket()
      {
        return socket_;
      }

      /// @brief Is this feed currently a member of the group?
      bool joined()const
      {
        return joined_;
      }

      /// @brief Is a read outstanding on this feed?
      bool readInProgress()const
      {
        return readInProgress_;
      }

      /// @brief Statistic: packets discarded because acceptSender() refused them.
      /// @returns the number of packets dropped on arrival.
      size_t rejectedPackets()const
      {
        return rejectedPackets_;
      }

    protected:
      /// @brief Join the multicast group on the open socket.
      ///
      /// Called with the socket already bound.  Throw to refuse the join.
      virtual void join() = 0;

      /// @brief Leave the multicast group.
      ///
      /// Called only when the feed believes it is joined.
      virtual void leave() = 0;

      /// @brief Should a packet from this sender be delivered?
      ///
      /// Called for every non-empty packet.  The default accepts everything
      /// the network stack delivered, which is right when the subscription
      /// itself already says who may send.
      ///
      /// @returns true to deliver the packet, false to discard it.
      virtual bool acceptSender(const asio::ip::udp::endpoint &) const
      {
        return true;
      }

    private:
      MulticastFeedBase();
      MulticastFeedBase(const MulticastFeedBase &);
      MulticastFeedBase & operator =(const MulticastFeedBase &);

      void handleReceive(
        const asio::error_code& error,
        LinkedBuffer * buffer,
        size_t bytesReceived)
      {
        assert(readInProgress_);
        readInProgress_ = false;
        size_t accepted = bytesReceived;
        if(!error && bytesReceived > 0 && !acceptSender(senderEndpoint_))
        {
          // Reported as an empty read so the host recycles the buffer and
          // starts the next read; rejectedPackets_ is what says why.
          ++rejectedPackets_;
          accepted = 0;
        }
        // Tag the buffer with this feed's name_ ("A" / "B") so the host can
        // identify which redundant feed delivered the packet after A/B merge.
        // Pointer to name_ is safe: the feed outlives every buffer it tags.
        if(!error && accepted > 0)
        {
          buffer->setExtra(&name_);
        }
        host_.feedReceived(error, buffer, accepted);
        if(host_.feedStopping())
        {
          if(joined_)
          {
            leave();
            joined_ = false;
          }
          socket_.close();
        }
      }

    private:
      MulticastFeedHost & host_;
      std::string name_;
      asio::ip::address listenInterface_;
      unsigned short portNumber_;
      asio::ip::address multicastGroup_;
      asio::ip::address bindAddress_;
      asio::ip::udp::endpoint endpoint_;
      asio::ip::udp::endpoint senderEndpoint_;
      asio::ip::udp::socket socket_;
      bool joined_;
      bool readInProgress_;
      Receiver::Statistic rejectedPackets_;
    };

    /// @brief smart pointer to a MulticastFeedBase
    typedef std::shared_ptr<MulticastFeedBase> MulticastFeedPtr;
    /// @brief the feeds belonging to one receiver
    typedef std::vector<MulticastFeedPtr> MulticastFeedVector;
  }
}
#endif // MULTICASTFEEDBASE_H
