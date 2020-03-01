from collections import deque, defaultdict
import multiprocessing
from multiprocessing import Queue, Process, Manager
import threading
import time
import random

def observer(channels, lock, ):
    # set up channels
    print("Logger set up!")


    while True:
        if not channels["m"]["o"]:
            time.sleep(0.1)
        else:
            msg_type, msg = channels["m"]["o"].pop()
            if msg_type == "STOP":
                break
            elif msg_type == "BEGINSNAPSHOT":
                node_id = int(msg[0])
                channels["o"][node_id].appendleft(["SNAPSHOT", []])
            elif msg_type == "COLLECTSTATE":
                for recv_id, out_channel in channels["o"].items():
                    if recv_id == "m":
                        continue
                    out_channel.appendleft(["COLLECT", []])
                snapshot_state, snapshot_channels = {}, defaultdict(dict)
                for send_id, _ in channels["o"].items():
                    if send_id == "m":
                        continue
                    while len(channels[send_id]["o"]) <= 0:
                        time.sleep(0.01)
                    send_state, send_channel_state = channels[send_id]["o"].pop()
                    snapshot_state[send_id] = send_state
                    for in_id in send_channel_state.keys():
                        if in_id == "m" or in_id == "o":
                            continue
                        snapshot_channels[in_id][send_id] = send_channel_state[in_id]
                channels["o"]["m"].appendleft([snapshot_state, snapshot_channels])
            print("this is the msg_type {} with message {}".format(msg_type, msg))


def member_node(channels, lock, node_id, init_money):
    money = init_money
    node_id = node_id
    is_recording = {} 
    num_channels_recording = 0
    snapshot_money = []
    snapshot_channels = {}
    
    print("member_node set up!  node_id {}, money {}".format(node_id, money))

    while True:
        if not (channels["m"][node_id]) and not (channels["o"][node_id]):
            time.sleep(0.1)
        elif channels["o"][node_id]:
            msg_type, msg = channels["o"][node_id].pop()
            if msg_type == "SNAPSHOT":
                snapshot_money = money
                num_channels_recording = 0
                for nb_id, out_channel in channels[node_id].items():
                    if nb_id == 'm' or nb_id == 'o':
                        continue
                    is_recording[nb_id] = True
                    out_channel.appendleft(['M', []]) # send Marker
                    snapshot_channels[nb_id] = 0 # init snapshot memory
                    num_channels_recording += 1
            elif msg_type == "COLLECT":
                num_channels_recording = 0
                for nb_id, out_channel in channels[node_id].items():
                    if nb_id == 'm' or nb_id == 'o':
                        continue
                    is_recording[nb_id] = False
                channels[node_id]["o"].appendleft([snapshot_money, snapshot_channels])
        else:
            print("specific channel state is ", channels['m'][node_id])
            msg_type, msg = channels["m"][node_id].pop()
            if msg_type == "STOP":
                break
            elif msg_type == "SEND":
                # TODO: ERR_SEND
                recv_id, amount = msg[0], msg[1]
                print("sending money.... to {}, with money {}".format(recv_id, amount))
                money -= amount
                channels[node_id][recv_id].appendleft(["X", [node_id, amount]])
            elif msg_type == "RECV":
                send_id = msg[0]
                if not channels[send_id][node_id]:
                    continue
                # while not channels[send_id][node_id]:
                #     time.sleep(0.1)
                lock.acquire()
                msg_flag, msg = channels[send_id][node_id].pop()
                if msg_flag == "M":
                    if num_channels_recording == 0:
                        snapshot_money = money
                        for nb_id, out_channel in channels[node_id].items():
                            if nb_id == 'm' or nb_id == 'o':
                                continue
                            is_recording[nb_id] = True
                            out_channel.appendleft(['M', []]) # send Marker
                            snapshot_channels[nb_id] = 0 # init snapshot memory
                            num_channels_recording += 1
                    else:
                        if is_recording[send_id]:
                            is_recording[send_id] = False
                            num_channels_recording -= 1
                        # otherwise ignore
                else:
                    sender_id, amount = msg
                    if is_recording.get(sender_id, False):
                        snapshot_channels[sender_id] += amount
                    money += amount
                    print("receiving money ... from {}, with money {}".format(sender_id, amount))
                
                lock.release()


def receive_all_helper(channels, lock):
    empty = True
    src, dst = None, None
    # this is not efficient, but just to fulfill the requirement
    nonempty_map = []
    lock.acquire()
    for src, row in channels.items():
        for dst, channel in row.items():
            if len(channel) != 0:
                empty = False
                nonempty_map.append((src, dst))
    lock.release()

    
    if not empty:
        src, dst = random.choice(nonempty_map)
        print("still channels not empty ... src {} dst {}".format(src, dst))
    else:
        print("all channels empty!")
    
    return empty, src, dst



def main():
    #Set up
    thread_map = {}
    channels = {}
    member_list = []
    lock = threading.RLock()
    #manager = Manager()
    #lock = manager.RLock()
    #channels = manager.dict()
    print("Please enter a command: ")
    while True:
        command = input()
        command_list = command.split()
        print("command recieved: ", command_list)
        if len(command_list) == 0:
            continue
        if command_list[0] == "StartMaster":
            lock.acquire()
            channels["m"] = {"o": deque()}
            channels["o"] = {"m": deque()}
            lock.release()
            observer_thread = threading.Thread(target=observer, args=(channels, lock))
            thread_map["observer"] = observer_thread
            observer_thread.start()
            

        if command_list[0] == "CreateNode":
            node_id = int(command_list[1])
            init_money = int(command_list[2])

            # set up channels
            lock.acquire()
            for _, src in channels.items():
                src[node_id] = deque()
            new_channels = {"m": deque(), "o": deque()}
            for member in member_list:
                new_channels[member] = deque()
            channels[node_id] = new_channels
            member_list.append(node_id)
            print("channels list is")
            print(channels)
            lock.release()

            new_node = threading.Thread(target=member_node, args=(channels, lock, node_id, init_money))
            thread_map[node_id] = new_node
            new_node.start()

        if command_list[0] == "test":
            channels['m']['o'].appendleft(["hello", "world"])

        
        if command_list[0] == "Send":
            _, src_id, dst_id, money = command_list
            channels["m"][int(src_id)].appendleft(["SEND", [int(dst_id), int(money)]])
        
        if command_list[0] == "Receive":
            recv_id = int(command_list[1])
            if len(command_list) == 3:
                send_id = command_list[2]
            else:
                send_id = random.choice(member_list)
                assert len(member_list) > 1, "no members to randomly send to!"
                while send_id == recv_id:
                    send_id = random.choice(member_list)
            
            channels["m"][int(recv_id)].appendleft(["RECV", [int(send_id)]])
        
        if command_list[0] == "ReceiveAll":
            empty, src, dst = receive_all_helper(channels,lock)
            while not empty:
                channels["m"][int(dst)].appendleft(["RECV", [src]])
                time.sleep(0.1)
                empty, src, dst = receive_all_helper(channels, lock)

        if command_list[0] == "KillAll":
            print("killing all processes...")
            # send killing command
            lock.acquire()
            for _, channel in channels["m"].items():
                channel.appendleft(["STOP", "STOP"])
            for _, v in thread_map.items():
                v.join()
            
            print("finished!")
            break

        if command_list[0] == "BeginSnapshot":
            node_id = int(command_list[1])
            channels["m"]["o"].appendleft(["BEGINSNAPSHOT", [node_id]])

        if command_list[0] == "CollectState":
            channels["m"]["o"].appendleft(["COLLECTSTATE", []])
            while len(channels["o"]["m"]) <= 0:
                continue
            snapshot_state, snapshot_channels = channels["o"]["m"].pop()
            print(snapshot_state)
            print(snapshot_channels)

        if command_list[0] == "q":
            return
        else:
            continue
            # print("Unknown command, issue a new command!")



    
    return

    
    


if __name__ == "__main__":
    main()