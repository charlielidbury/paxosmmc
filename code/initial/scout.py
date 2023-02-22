from process import Process
from message import BallotRequest,BallotResponse,PreemptedMessage,AdoptSuccess

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
      self.sendMessage(acceptor, BallotRequest(self.id, self.ballot_number))
      pendingAcceptors.add(acceptor)

    pvalues = set()
    while True:
      msg = self.getNextMessage()
      if isinstance(msg, BallotResponse):
        
        if self.ballot_number == msg.ballot_number and msg.src in pendingAcceptors:
          # Ballot has been accepted

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
          # Ballot has been rejected by at least one acceptor

          # Preempt the leader and kill this scout process
          self.sendMessage(
            self.leader,
            PreemptedMessage(
              self.id,
              msg.ballot_number
            )
          )
          return
      else:
        print("Scout: unexpected msg")

