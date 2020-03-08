from collections import deque, defaultdict
from sys import stdin


class Message:
    """
    class for messages sent in channels
    """
    def __init__(self, msg, value):
        self.type = msg
        self.value = value

class Node:
    def __init__(self, id, money):
        """
        initialize a node
        use other methods to set up channels
        """ 
        self.id = id
        self.money = money
        self.nodes_in = defaultdict(deque)
        self.nodes_out = defaultdict(deque)
        self.observer_in = deque()
        self.observer_out = deque()
        
        # initialize snapshot states
        self.state = None
        self.remain_recording = 0
        self.channel_finished = defaultdict(bool)
        self.channel_state = defaultdict(int)

    def connect_observer(self, observer_in, observer_out):
        """
        set up channels with the observer
        """
        self.observer_in = observer_in
        self.observer_out = observer_out

    def connect_node(self, id, node_in, node_out):
        """
        set up channels with another node with id
        """
        self.nodes_in[id] = node_in
        self.nodes_out[id] = node_out

    def kill(self):
        """
        dummy cleanup since we use single thread here
        """
        return

    def send(self, receiver, money):
        """
        send money to receiver with id
        """
        if money > self.money:
            print('ERR_SEND')
            return
        self.money -= money
        msg = Message('Transfer', money)
        self.nodes_out[receiver].appendleft(msg)

    def receive(self, sender=-1, output=True):
        """
        receive msg from sender with id
        pick a random channel with msg to receive if sender not specified
        """
        if sender != -1 and not self.nodes_in[sender]:
            return
        if sender == -1:
            # find an in-channel with message
            for id in self.nodes_in:
                if self.nodes_in[id]:
                    sender = id
            if sender == -1:
                return

        msg = self.nodes_in[sender].pop()
        if msg.type == 'SnapshotToken':
            # check whether the node started recording already
            if not self.remain_recording:
                self._startSnapshot(sender)
            else:
                if not self.channel_finished[sender]:
                    self.channel_finished[sender] = True
                    self.remain_recording -= 1
            # check whether snapshot is finished
            if not self.remain_recording:
                self._endSnapshot()
        elif msg.type == 'Transfer':
            self.money += msg.value
            # check whether is recording along this channel
            if self.remain_recording and not self.channel_finished[sender]:
                self.channel_state[sender] += msg.value
        else:
            return
        if output:
            print('{:d} {:s} {:d}'.format(sender, msg.type, msg.value))

    def receiveAll(self):
        """
        receive msg from all senders
        """
        for sender in self.nodes_in:
            while self.nodes_in[sender]:
                self.receive(sender, False)

    def receiveObserver(self):
        """
        receive msg from observer
        """
        if not self.observer_in:
            return
        msg = self.observer_in.pop()
        if msg.type == 'TakeSnapshot':
            self._startSnapshot()

    def sendObserver(self, msg):
        """
        send msg to observer
        """
        self.observer_out.appendleft(msg)

    def _startSnapshot(self, sender=-1):
        """
        start a snapshot
        assume the number of nodes is fixed during a snapshot
        """
        
        self.state = self.money
        self.remain_recording = len(self.nodes_in)
        if sender != -1: # received marker from another node
            self.remain_recording -= 1
            self.channel_finished[sender] = True
            self.channel_state[sender] = 0
        else:
            print('Started by Node {:d}'.format(self.id))
        msg = Message('SnapshotToken', -1)
        for receiver in self.nodes_out:
            self.nodes_out[receiver].appendleft(msg)

    def _endSnapshot(self):
        """
        end a snapshot and send states to observer
        restore snapshot states
        """
        self.sendObserver(Message('NodeState', self.state))
        self.sendObserver(Message('ChannelState', self.channel_state))
        self.state = None
        self.remain_recording = 0
        self.channel_finished = defaultdict(bool)
        self.channel_state = defaultdict(int)


class Observer:
    def __init__(self):
        """
        initialize the observer
        use other methods to set up channels
        """
        self.nodes_in = defaultdict(deque)
        self.nodes_out = defaultdict(deque)
        self.node_states = defaultdict(int)
        self.channel_states = defaultdict(int)
        self.snapshot = False

    def connect_node(self, id, node_in, node_out):
        """
        set up channels with a node with id
        """
        self.nodes_in[id] = node_in
        self.nodes_out[id] = node_out

    def beginSnapshot(self, id):
        """
        ask node with id to take a snapshot
        """
        if self.snapshot:
            return
        msg = Message('TakeSnapshot', -1)
        self.nodes_out[id].appendleft(msg)
        self.snapshot = True

    def collectState(self):
        """
        collect snapshot states from nodes
        """
        if not self.snapshot:
            return
        for id in self.nodes_in:
            while self.nodes_in[id]:
                msg = self.nodes_in[id].pop()
                if msg.type == 'NodeState':
                    self.node_states[id] = msg.value
                elif msg.type == 'ChannelState':
                    for sender in msg.value:
                        self.channel_states[(sender, id)] = msg.value[sender]
        self.snapshot = False

    def printSnapshot(self):
        """
        print collected snapshot
        restore saved states
        """
        print('---Node states')
        for id in sorted(self.node_states.keys()):
            print('node {:d} = {:d}'.format(id, self.node_states[id]))
        print('---Channel states')
        for pair in sorted(self.channel_states.keys()):
            print('channel ({:d} -> {:d}) = {:d}'.format(pair[0], pair[1], self.channel_states[pair]))
        
        self.node_states = defaultdict(int)
        self.channel_states = defaultdict(int)
        self.snapshot = False


class Master:
    def setup(self):
        """
        set up helper function for initialization and clean up
        """
        # set up the observer by default
        self.observer = Observer()
        self.nodes = {}
        # set up channels
        self.channels = {
            'o2n': defaultdict(deque),
            'n2o': defaultdict(deque),
            'n2n': defaultdict(deque)
        }
    
    def killAll(self):
        """
        kill all nodes and clean up
        """
        for node in self.nodes:
            node.kill()
        self.setup()

    def createNode(self, id, money):
        node = Node(id, money)
        self.nodes[id] = node
        # set up observer connection
        node.connect_observer(self.channels['o2n'][id], self.channels['n2o'][id])
        self.observer.connect_node(id, self.channels['n2o'][id], self.channels['o2n'][id])
        # set up node connection
        for id2 in self.nodes:
            if id2 != id:
                self.nodes[id2].connect_node(id, self.channels['n2n'][(id, id2)], self.channels['n2n'][(id2, id)])
                self.nodes[id].connect_node(id2, self.channels['n2n'][(id2, id)], self.channels['n2n'][(id, id2)])

    def send(self, sender, receiver, money):
        self.nodes[sender].send(receiver, money)

    def receive(self, receiver, sender=-1):
        self.nodes[receiver].receive(sender)

    def receiveAll(self):
        for id in self.nodes:
            self.nodes[id].receiveAll()
    
    def beginSnapshot(self, id):
        self.observer.beginSnapshot(id)
        # ensure blocking by asking the node to receive msg
        self.nodes[id].receiveObserver()

    def collectState(self):
        self.observer.collectState()

    def printSnapshot(self):
        self.observer.printSnapshot()

def main():
    master = Master()

    for command in stdin:
        command_list = command.split()
        if not command_list:
            continue

        if command_list[0] == 'StartMaster':
            master.setup()

        elif command_list[0] == 'KillAll':
            master.killAll()

        elif command_list[0] == 'CreateNode':
            id = int(command_list[1])
            money = int(command_list[2])
            master.createNode(id, money)

        elif command_list[0] == 'Send':
            sender = int(command_list[1])
            receiver = int(command_list[2])
            money = int(command_list[3])
            master.send(sender, receiver, money)

        elif command_list[0] == 'Receive':
            receiver = int(command_list[1])
            sender = -1
            if len(command_list) == 3:
                sender = int(command_list[2])
            master.receive(receiver, sender)

        elif command_list[0] == 'ReceiveAll':
            master.receiveAll()

        elif command_list[0] == 'BeginSnapshot':
            id = int(command_list[1])
            master.beginSnapshot(id)

        elif command_list[0] == 'CollectState':
            master.collectState()

        elif command_list[0] == 'PrintSnapshot':
            master.printSnapshot()


if __name__ == "__main__":
    main()