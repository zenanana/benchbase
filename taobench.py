# shards to keys distribution
read_percent = 0.0
edge_percent = 0.5
shard_size = 10
repeating = False
readTxnSizes = [0, 104440, 283626, 340601, 43853, 5878, 107888, 674, 862, 235, 409, 196, 409, 150, 332, 20, 288, 12, 494, 10, 51871, 7, 17, 8, 15, 20160, 6, 0, 7, 3, 7, 1, 8, 3, 4, 1, 6, 0, 10, 0, 8, 1, 1, 0, 1, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]
writeTxnSizes = [0, 553162, 311751, 166458, 106694, 80485, 62975, 53430, 33268, 27819, 36732, 21447, 18031, 15122, 14191, 12506, 10894, 10053, 9326, 8482, 7752, 7189, 6773, 6265, 5995, 5982, 5503, 5473, 4896, 5001, 9323, 4619, 4773, 4323, 3927, 4634, 4598, 4356, 3803, 3888, 3814, 3544, 3498, 3422, 3373, 3057, 3038, 3220, 3100, 2961, 2824, 2687, 2780, 2796, 2831, 2622, 2576, 2336, 2090, 2160, 2074, 2062, 2054, 2075, 1980, 2231, 2105, 2052, 2006, 2088, 1846, 1871, 1925, 1766, 1649, 1751, 1651, 2337, 1736, 1602, 1527, 1504, 1447, 1493, 1336, 1438, 1332, 1503, 1310, 1397, 1372, 1497, 1370, 1376, 1406, 1535, 1605, 1646, 1658, 1846, 1266, 1235, 1199, 1147, 1066, 1099, 1008, 1082, 966, 1023, 847, 941, 1034, 857, 795, 662, 714, 719, 619, 606, 617, 617, 588, 576, 590, 612, 537, 654, 664, 679, 539, 570, 519, 508, 562, 539, 476, 493, 510, 557, 531, 518, 522, 527, 518, 508, 492, 537, 574, 497, 582, 558, 560, 488, 585, 521, 603, 556, 516, 578, 487, 523, 512, 495, 530, 578, 554, 532, 576, 596, 520, 525, 508, 615, 548, 444, 592, 458, 561, 524, 569, 538, 519, 635, 462, 921, 446, 472, 400, 407, 399, 445, 394, 547, 437, 442, 416, 406, 392, 332, 304, 265, 252, 243, 259, 245, 246, 274, 190, 260, 271, 230, 206, 212, 213, 208, 209, 249, 214, 240, 214, 204, 218, 205, 199, 205, 193, 173, 186, 170, 219, 168, 204, 191, 173, 170, 171, 222, 190, 172, 185, 204, 172, 171, 204, 146, 167, 186, 140, 137, 151, 137, 122, 152, 139, 151, 102, 105, 128, 202, 127, 125, 116, 121, 146, 110, 127, 80, 104, 120, 113, 117, 131, 131, 111, 100, 97, 110, 99, 123, 84, 107, 134, 74, 79, 110, 105, 110, 63, 75, 106, 92, 89, 101, 95, 84, 88, 87, 80, 92, 101, 92, 81, 83, 73, 61, 79, 114, 76, 70, 67, 110, 93, 81, 79, 70, 73, 86, 92, 78, 71, 83, 71, 71, 87, 79, 88, 78, 102, 102, 90, 93, 68, 68, 72, 113, 88, 107, 79, 72, 58, 75, 57, 70, 48, 61, 48, 73, 40, 63, 74, 61, 72, 58, 61, 62, 51, 48, 63, 49, 65, 39, 78, 70, 64, 54, 55, 59, 51, 61, 64, 56, 44, 45, 76, 63, 62, 56, 65, 67, 63, 61, 57, 79, 92, 76, 81, 96, 71, 117, 96, 78, 59, 83, 53, 65, 77, 44, 48, 56, 57, 50, 63, 57, 40, 62, 28, 61, 45, 63, 52, 27, 66, 53, 58, 59, 60, 56, 51, 78, 47, 54, 50, 50, 27, 36, 57, 46, 62, 53, 38, 39, 46, 54, 51, 29, 30, 39, 37, 42, 52, 39, 42, 58, 38, 36, 36, 95, 29, 45, 26, 48, 37, 29, 58, 50, 34, 38, 29, 42, 44, 32, 37, 28, 27, 31, 30, 47, 34, 57, 40, 34, 52, 48, 23, 45, 22, 33, 38, 29, 37, 27, 27, 41, 40, 36, 40, 41, 53, 32, 39, 45, 27, 70, 48, 41, 43, 18, 28, 44]
primaryShards = [94036,36224,3600,612,612,612,612,612,614,612,612,612,612,612,612,612,614,612,612,612,612,612,612,612,612,614,612,612,612,612,612,612,612,614,612,612,612,612,612,612,612,614,612,612,612,612,612,612,612,600]
remoteShards = [31712,2563,1925,1722,1663,1620,1037,679,381,247,176,149,130,119,112,99,96,89,80,80,76,64,64,64,64,48,48,48,48,48,45,32,32,32,32,32,32,32,32,17,16,16,16,16,16,16,16,16,16,15]
def formatWeights(arr):
    total = np.sum(arr)
    new_weights = []
    for x in arr:
        new_weights.append(x * 1.0 / total)
    return new_weights
readTxnSizeWeights = formatWeights(readTxnSizes)
writeTxnSizeWeights = formatWeights(writeTxnSizes)
primaryShardWeights = formatWeights(primaryShards)
remoteShardWeights = formatWeights(remoteShards)
readTxnSizeDistribution = stats.rv_discrete(values=(np.arange(len(readTxnSizeWeights)),readTxnSizeWeights))
writeTxnSizeDistribution = stats.rv_discrete(values=(np.arange(len(writeTxnSizeWeights)),writeTxnSizeWeights))
primaryShardDistribution = stats.rv_discrete(values=(np.arange(len(primaryShardWeights)),primaryShardWeights))
remoteShardDistribution = stats.rv_discrete(values=(np.arange(len(remoteShardWeights)),remoteShardWeights))
a = 2
read_percent4 = 0.0
edge_percent4 = 0.5
shard_size4 = 1000
repeating4 = False
def gen_taobench_workload4(num_txns): #, shard_size4
    workload_str = "{"
    for i in range(num_txns):
        workload_str += '"txn' + str(i) + '"' + ":"
        num_ops = random.randint(20,85) #writeTxnSizeDistribution.rvs()#
        # if random.random() < read_percent3:
        #     num_ops = readTxnSizeDistribution.rvs()
        # else:
        #     num_ops = writeTxnSizeDistribution.rvs()
        ops_str = '"'
        keys = set() #[] #
        for j in range(num_ops):
            op = ""
            primaryShard = j
            remoteShard = j
            # if i % 2 == 0:
            #     primaryShard = j + 100
            #     remoteShard = j + 100
            if random.random() < edge_percent4:
                k = ""
                rk = ""
                if j < 3:
                    k = str(zipf.rvs(a))
                    rk = str(zipf.rvs(a))
                else:
                    k = str(random.randint(1, shard_size4))
                    rk = str(random.randint(1, shard_size4))
                primaryKey = str(primaryShard) + ":" + k
                remoteKey = str(remoteShard) + ":" + rk
                op = primaryKey + ":" + remoteKey
                if not repeating4:
                    while op in keys:
                        if j < 3:
                            k = str(zipf.rvs(a))
                            rk = str(zipf.rvs(a))
                        else:
                            k = str(random.randint(1, shard_size4))
                            rk = str(random.randint(1, shard_size4))
                        primaryKey = str(primaryShard) + ":" + k
                        remoteKey = str(remoteShard) + ":" + rk
                        op = primaryKey + ":" + remoteKey
                    keys.add(op) #append(op)#.
            else:
                k = ""
                if j < 3:
                    k = str(zipf.rvs(a))
                else:
                    k = str(random.randint(1, shard_size4))
                primaryKey = str(primaryShard) + ":" + k
                op = primaryKey
                if not repeating4:
                    while op in keys:
                        if j < 3:
                            k = str(zipf.rvs(a))
                        else:
                            k = str(random.randint(1, shard_size4))
                        primaryKey = str(primaryShard) + ":" + k
                        op = primaryKey
                    keys.add(op) #append(op)#.

        list_keys = list(keys)
        # print(list_keys)
        num_write_ops = random.randint(1,10)
        write_indicies = []
        count_writes = 0
        while count_writes < num_write_ops:
            id = random.randint(0, len(list_keys) - 1) #zipf.rvs(a)#
            while id in write_indicies: # or id > len(list_keys)
                id = random.randint(0, len(list_keys) - 1)
            write_indicies.append(id)
            count_writes += 1
        # print(num_write_ops, write_indicies, list_keys)
        indices = random.sample(range(len(list_keys)), len(list_keys))
        for j in range(num_ops):
            op = list_keys[indices[j]]
            op = 'r-' + op
            # if random.randint(0,1) < read_percent4:
            #     op = 'r-' + op
            # else:
            #     op = 'w-' + op
            ops_str += op + " "
        # print(ops_str)

        for j in range(num_write_ops):
            op = list_keys[write_indicies[j]]
            op = 'w-' + op
            ops_str += op + " "
        ops_str = ops_str[:-1]
        ops_str += '"'
        workload_str += ops_str
        if i != num_txns -1:
            workload_str += ", "
        else:
            workload_str += "}"
    return workload_str
