from message import P2aMessage,P2bMessage,PreemptedMessage,DecisionMessage
from process import Process
from utils import Command

# Responsible for a single command and slot proposal
class Commander(Process):
  def __init__(self, env, id, leader, acceptors, replicas,
               ballot_number, slot_number, command):
    Process.__init__(self, env, id)
    self.leader = leader
    self.acceptors = acceptors
    self.replicas = replicas
    self.ballot_number = ballot_number
    self.slot_number = slot_number
    self.command = command
    self.env.addProc(self)

  def body(self):
    waitfor = set()

    # Send the P2a message to all acceptors
    for acceptor in self.acceptors:
      self.sendMessage(
        acceptor, 
        P2aMessage(
          self.id, 
          self.ballot_number,
          self.slot_number, 
          self.command
        )
      )

      waitfor.add(acceptor)

    while True:
      msg = self.getNextMessage()
      if isinstance(msg, P2bMessage):

        if self.ballot_number == msg.ballot_number and msg.src in waitfor:
          waitfor.remove(msg.src)

          if len(waitfor) < float(len(self.acceptors)) / 2:
            for replica in self.replicas:
              self.sendMessage(
                replica, 
                DecisionMessage(
                  self.id,
                  self.slot_number,
                  self.command
                )
              )

            return
        else:
          self.sendMessage(
            self.leader, 
            PreemptedMessage(
              self.id,
              msg.ballot_number
            )
          )
          return


