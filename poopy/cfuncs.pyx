"""
This module contains functions for building a topological stack of nodes in a flow direction array.
For speed and memory efficiency, the functions are written in Cython.
"""

import numpy as np
cimport numpy as np
cimport cython
from libc.stdlib cimport malloc, free

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def d8_to_receivers(np.ndarray[long, ndim=2] arr) -> long[:] :
    """
    Converts a D8 flow direction array to a receiver array.

    Args:
        arr: A D8 flow direction array.
    
    Returns:
        A receiver array.
    """
    cdef int nrows = arr.shape[0]
    cdef int ncols = arr.shape[1]
    cdef long[:] receivers = np.empty(nrows * ncols, dtype=long)
    cdef int i, j
    cdef int cell
    for i in range(nrows):
        for j in range(ncols):
            cell = i * ncols + j
            # Check if boundary cell
            if i == 0 or j == 0 or i == nrows - 1 or j == ncols - 1 or arr[i, j] == 0:
                receivers[cell] = cell
            elif arr[i, j] == 1:  # Right
                receivers[cell] = i * ncols + j + 1
            elif arr[i, j] == 2:  # Lower right
                receivers[cell] = (i + 1) * ncols + j + 1
            elif arr[i, j] == 4:  # Bottom
                receivers[cell] = (i + 1) * ncols + j
            elif arr[i, j] == 8:  # Lower left
                receivers[cell] = (i + 1) * ncols + j - 1
            elif arr[i, j] == 16:  # Left
                receivers[cell] = i * ncols + j - 1
            elif arr[i, j] == 32:  # Upper left
                receivers[cell] = (i - 1) * ncols + j - 1
            elif arr[i, j] == 64:  # Top
                receivers[cell] = (i - 1) * ncols + j
            elif arr[i, j] == 128:  # Upper right
                receivers[cell] = (i - 1) * ncols + j + 1
            else:
                raise ValueError(f"Invalid flow direction value: {arr[i, j]}")
    return receivers


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def count_donors(long[:] r) -> int[:] :
    """
    Counts the number of donors that each cell has.

    Args:
        r: The receiver indices.

    Returns:
        An array of donor counts.
    """
    cdef int n = len(r)  # np = number of pixels
    cdef int[:] d = np.zeros(n, dtype=np.int32)
    cdef int j
    for j in range(n):
        d[r[j]] += 1
    return d    

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def ndonors_to_delta(int[:] nd) -> int[:] :
    """
    Converts a number of donors array to an index array that contains the location of where the list of
    donors to node i is stored.

    Args:
        nd: The donor array.

    Returns:
        An array of donor counts.
    """
    cdef int n = len(nd)
    # Initialize the index array to the number of pixels
    cdef int[:] delta = np.zeros(n + 1, dtype=np.int32)
    delta[n] = n
    cdef int i
    for i in range(n, -1, -1):
        if i == n:
            continue
        delta[i] = delta[i + 1] - nd[i]

    return delta    

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def make_donor_array(long[:] r, int[:] delta) -> int[:] :
    """
    Makes the array of donors. This is indexed according to the delta
    array. i.e., the donors to node i are stored in the range delta[i] to delta[i+1].
    So, to extract the donors to node i, you would do:
    donors[delta[i]:delta[i+1]]

    Args:
        r: The receiver indices.
        delta: The delta index array.

    Returns:
        The donor array.
    """
    cdef int n = len(r)  # np = number of pixels
    # Define an integer working array w intialised to 0.
    cdef int[:] w = np.zeros(n, dtype=np.int32)
    # Donor array D
    cdef int[:] D = np.zeros(n, dtype=np.int32)
    cdef int i
    for i in range(n):
        D[delta[r[i]] + w[r[i]]] = i
        w[r[i]] += 1

    return D    


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def add_to_stack(int l, int j, int[:] s, int[:] delta, int[:] donors) -> int:
    """
    Adds node l, and its donors (recursively), to the stack. Used in the recursive
    build_stack function.

    Args:
        l: The node index.
        j: The stack index.
        s: The stack.
        delta: The index array.
        donors: The donor information array.

    Returns:
        The updated stack index.
    """
    s[j] = l
    j += 1
    cdef int n, m
    for n in range(delta[l], delta[l + 1]):
        m = donors[n]
        if m != l:
            j = add_to_stack(m, j, s, delta, donors)

    return j

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def build_stack_recursive(long[:] receivers, np.ndarray[long, ndim=1] baselevel_nodes) -> int[:] :
    """
    Builds the stack of nodes in topological order, given the receiver array.
    Starts at the baselevel nodes and works upstream. This uses recursion 
    and as such is not recommended for large arrays. Included for legacy reasons. 
    Use build_stack_iterative instead.

    Args:
        receivers: The receiver array (i.e., receiver[i] is the ID
        of the node that receives the flow from the i'th node).
        baselevel_nodes: The baselevel nodes to start from.

    Returns:
        The stack of nodes in topological order.
    """
    cdef int n = len(receivers)
    cdef int[:] n_donors = count_donors(receivers)
    cdef int[:] delta = ndonors_to_delta(n_donors)
    cdef int[:] donors = make_donor_array(receivers, delta)
    cdef int[:] stack = np.zeros(n, dtype=np.int32) - 1
    cdef int j = 0
    cdef int b
    for b in baselevel_nodes:
        j = add_to_stack(b, j, stack, delta, donors)
    return stack      

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def build_stack_iterative(long[:] receivers, np.ndarray[long, ndim=1] baselevel_nodes) -> int[:] :
    """
    Builds the stack of nodes in topological order, given the receiver array.
    Starts at the baselevel nodes and works upstream in a wave building a 
    breadth-first search order of the nodes.

    Args:
        receivers: The receiver array (i.e., receiver[i] is the ID
        of the node that receives the flow from the i'th node).
        baselevel_nodes: The baselevel nodes to start from.

    Returns:
        The stack of nodes in topological order (BFS).
    """
    cdef int n = len(receivers)
    cdef int[:] n_donors = count_donors(receivers)
    cdef int[:] delta = ndonors_to_delta(n_donors)
    cdef int[:] donors = make_donor_array(receivers, delta)
    cdef int[:] stack = np.zeros(n, dtype=np.int32) - 1
    cdef int j = 0 # The index in the stack (i.e., topological order)
    cdef int b, node, m
    # Queue for breadth-first search
    cdef int *q = <int *> malloc(n * sizeof(int))
    cdef int front = 0
    cdef int back = 0

    # Add baselevel nodes to the stack
    for b in baselevel_nodes:
        q[back] = b
        back += 1

    while front < back:
        node = q[front] # Get the node from the queue
        front += 1 # Increment the front of the queue (i.e., pop the node)
        stack[j] = node # Add the node to the stack
        j += 1 # Increment the stack index.
        # Loop through the donors of the node
        for n in range(delta[node], delta[node+1]):
            m = donors[n]
            if m != node:
                q[back] = m
                back += 1

    free(q)
    return stack

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def accumulate_flow(
    long[:] receivers, 
    int[:] stack, 
    np.ndarray[double, ndim=1] weights
):
    """
    Accumulates flow along the stack of nodes in topological order, given the receiver array,
    the ordered stack, and a weights array which contains the contribution from each node.

    Args:
        receivers: The receiver array (i.e., receiver[i] is the ID
        of the node that receives the flow from the i'th node).
        stack: The ordered stack of nodes.
        weights: The weights array (i.e., the contribution from each node).
    """
    cdef int n = receivers.shape[0]
    cdef np.ndarray[double, ndim=1] accum = weights.copy()
    cdef int i
    cdef long donor, recvr

    # Accumulate flow along the stack from upstream to downstream
    for i in range(n - 1, -1, -1):
        donor = stack[i]
        recvr = receivers[donor]
        if donor != recvr:
            accum[recvr] += accum[donor]

    return accum