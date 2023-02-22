from utils import BallotNumber
from process import Process
from commander import Commander
from scout import Scout
from message import ProposeMessage,AdoptSuccess,AdoptFailure

class Leader(Process):
  def __init__(self, env, id, config):
    Process.__init__(self, env, id)
    self.ballot_number = BallotNumber(0, self.id)
    self.active = False
    self.proposals = {}
    self.config = config
    self.env.addProc(self)

  def body(self):
    print("Here I am: ", self.id)

    # Spawn a scout for the current ballot number, operating on our acceptors
    Scout(
      self.env, 
      "scout:%s:%s" % (str(self.id), str(self.ballot_number)),
      self.id, 
      self.config.acceptors, 
      self.ballot_number
    )
    
    while True:
      msg = self.getNextMessage()
      if isinstance(msg, ProposeMessage):

        # If the slot number hasn't been propesed for yet
        if msg.slot_number not in self.proposals:

          # Insert the command into the proposals dict
          self.proposals[msg.slot_number] = msg.command

          # If we're in the active state, spawn a commander
          if self.active:
            Commander(
              self.env,
              "commander:%s:%s:%s" % (str(self.id), str(self.ballot_number), str(msg.slot_number)),
              self.id, 
              self.config.acceptors, 
              self.config.replicas,
              self.ballot_number, 
              msg.slot_number, 
              msg.command
            )
      elif isinstance(msg, AdoptSuccess):
        # The majority of acceptors have adopted the given ballot number
        if self.ballot_number == msg.ballot_number:
          # The ballot number is also new

          # Iterate through the accepted proposals, and update our own
          # to maintain the maximum ballot number for each slot
          pmax = {}
          for (ballot_number, slot_number, command) in msg.accepted:

            if slot_number not in pmax or pmax[slot_number] < ballot_number:

              pmax[slot_number] = ballot_number
              self.proposals[slot_number] = command

          for slot_number in self.proposals:
            Commander(
              self.env,
              "commander:%s:%s:%s" % (str(self.id), str(self.ballot_number), str(slot_number)),
              self.id, 
              self.config.acceptors, 
              self.config.replicas,
              self.ballot_number, 
              slot_number, 
              self.proposals.get(slot_number)
            )
          
          # Put ourselves, the leader, into the active state
          self.active = True
        
        # Otherwise, the ballot number was old, ignore the adoption

      elif isinstance(msg, AdoptFailure):
        # This should be an assertion
        if msg.ballot_number > self.ballot_number:

          # This leader will not receive a majority of acceptors, 
          # go into inactive state and start a new ballot
          self.active = False
          self.ballot_number = BallotNumber(
            msg.ballot_number.round + 1,
            self.id
          )

          # Spawn a new scout for the new ballot number
          Scout(
            self.env, 
            "scout:%s:%s" % (str(self.id), str(self.ballot_number)),
            self.id, 
            self.config.acceptors, 
            self.ballot_number
          )

      else:
        print("Leader: unknown msg type")
