import sys
import numpy as np
np.set_printoptions(suppress=True, threshold=np.inf)

if len(sys.argv) != 3:
    print ("Usage: python genToken.py num_nodes num_tokens")
    sys.exit(1)
num_nodes = int(sys.argv[1])
num_tokens = int(sys.argv[2])

print ("\n".join(['[Node {}]\ninitial_token: {}'.format(r + 1, ','.join([str(round((2**64 / (num_tokens * num_nodes)) * (t * num_nodes + r)) - 2**63) for t in range(num_tokens)])) for r in range(num_nodes)]))