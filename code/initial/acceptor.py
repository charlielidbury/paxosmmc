from utils import PValue
from process import Process
from message import BallotRequest,BallotResponse,CommandRequest,CommandResponse

class Acceptor(Process):
  def __init__(self, env, id):
    Process.__init__(self, env, id)
    self.ballot_number = None
    self.accepted = set()
    self.env.addProc(self)

  def body(self):
    print("Here I am: ", self.id)
    while True:
      msg = self.getNextMessage()
      if isinstance(msg, BallotRequest):
        # Received a P1a message

        if msg.ballot_number > self.ballot_number:
          self.ballot_number = msg.ballot_number

        self.sendMessage(
          msg.src,
          BallotResponse(
            self.id,
            self.ballot_number,
            self.accepted
          )
        )
      elif isinstance(msg, CommandRequest):
        if msg.ballot_number == self.ballot_number:
          # Accept the command

          self.accepted.add(
            PValue(
              msg.ballot_number,
              msg.slot_number,
              msg.command
            )
          )
        
        # Notify the commander of our 
        self.sendMessage(
          msg.src,
          CommandResponse(
            self.id,
            self.ballot_number,
            msg.slot_number
          )
        )
