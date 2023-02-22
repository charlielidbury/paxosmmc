from message import CommandRequest,CommandResponse,PreemptedMessage,DecisionNotification
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
        CommandRequest(
          self.id, 
          self.ballot_number,
          self.slot_number, 
          self.command
        )
      )

      waitfor.add(acceptor)

    while True:
      msg = self.getNextMessage()
      if isinstance(msg, CommandResponse):

        if self.ballot_number == msg.ballot_number and msg.src in waitfor:
          waitfor.remove(msg.src)

          # If we have a majority of acceptors, the decision has been made, alert replicas
          if len(waitfor) < float(len(self.acceptors)) / 2:
            for replica in self.replicas:
              self.sendMessage(
                replica, 
                DecisionNotification(
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


