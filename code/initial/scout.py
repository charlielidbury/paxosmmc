from process import Process
from message import P1aMessage,P1bMessage,AdoptFailure,AdoptSuccess

class Scout(Process):
  def __init__(self, env, id, leader, acceptors, ballot_number):
    Process.__init__(self, env, id)
    self.leader = leader
    self.acceptors = acceptors
    self.ballot_number = ballot_number
    self.env.addProc(self)

  def body(self):
    # P1a messages only get sent to acceptors on initialization
    pendingAcceptors = set()
    for acceptor in self.acceptors:
      self.sendMessage(acceptor, P1aMessage(self.id, self.ballot_number))
      pendingAcceptors.add(acceptor)

    pvalues = set()
    while True:
      msg = self.getNextMessage()
      if isinstance(msg, P1bMessage):
        
        if self.ballot_number == msg.ballot_number and msg.src in pendingAcceptors:
          # We received a P1b message from a pending acceptor for the current ballot number

          pvalues.update(msg.accepted)
          pendingAcceptors.remove(msg.src)

          # If we have a majority of acceptors, send AdoptedMessage
          if len(pendingAcceptors) < float(len(self.acceptors)) / 2:
            self.sendMessage(
              self.leader,
              AdoptSuccess(
                self.id,
                self.ballot_number,
                pvalues
              )
            )
            return
        else:
          # We received a P1b message from either an acceptor that is
          # not pending or has greater ballot number

          # Preempt the leader and kill this scout process
          self.sendMessage(
            self.leader,
            AdoptFailure(
              self.id,
              msg.ballot_number
            )
          )
          return
      else:
        print("Scout: unexpected msg")

