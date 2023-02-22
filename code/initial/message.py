class Message:
  def __init__(self, src):
    self.src = src

  def __str__(self):
    return str(self.__dict__)

class BallotRequest(Message):
  def __init__(self, src, ballot_number):
    Message.__init__(self, src)
    self.ballot_number = ballot_number

class BallotResponse(Message):
  def __init__(self, src, ballot_number, accepted):
    Message.__init__(self, src)
    self.ballot_number = ballot_number
    self.accepted = accepted

class CommandRequest(Message):
  def __init__(self, src, ballot_number, slot_number, command):
    Message.__init__(self, src)
    self.ballot_number = ballot_number
    self.slot_number = slot_number
    self.command = command

class CommandResponse(Message):
  def __init__(self, src, ballot_number, slot_number):
    Message.__init__(self, src)
    self.ballot_number = ballot_number
    self.slot_number = slot_number

class PreemptedMessage(Message):
  def __init__(self, src, ballot_number):
    Message.__init__(self, src)
    self.ballot_number = ballot_number

class AdoptSuccess(Message):
  def __init__(self, src, ballot_number, accepted):
    Message.__init__(self, src)
    self.ballot_number = ballot_number
    self.accepted = accepted

class DecisionNotification(Message):
  def __init__(self, src, slot_number, command):
    Message.__init__(self, src)
    self.slot_number = slot_number
    self.command = command

class RequestMessage(Message):
  def __init__(self, src, command):
    Message.__init__(self, src)
    self.command = command

class ProposeMessage(Message):
  def __init__(self, src, slot_number, command):
    Message.__init__(self, src)
    self.slot_number = slot_number
    self.command = command
