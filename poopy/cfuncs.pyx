"""
This module contains functions for building a topological stack of nodes in a flow direction array.
For speed and memory efficiency, the functions are written in Cython. 
"""
# distutils: language = c++
from libcpp.stack cimport stack
from libcpp.vector cimport vector

import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdlib cimport malloc, free

@cython.boundscheck(False)
@cython.wraparound(False)
def d8_to_receivers(cnp.ndarray[cnp.int64_t, ndim=2] arr) -> cnp.int64_t[:]:
    """
    Converts a D8 flow direction array to a receiver array.

    Args:
        arr: A D8 flow direction array.

    Returns:
        A receiver array.
    """
    cdef Py_ssize_t nrows = arr.shape[0]
    cdef Py_ssize_t ncols = arr.shape[1]
    cdef cnp.int64_t[:] receivers = np.empty(nrows * ncols, dtype=np.int64)
    cdef Py_ssize_t i, j
    cdef Py_ssize_t cell

    for i in range(nrows):
        for j in range(ncols):
            cell = i * ncols + j
            if i == 0 or j == 0 or i == nrows - 1 or j == ncols - 1 or arr[i, j] == 0:
                receivers[cell] = cell
            elif arr[i, j] == 1:
                receivers[cell] = i * ncols + j + 1
            elif arr[i, j] == 2:
                receivers[cell] = (i + 1) * ncols + j + 1
            elif arr[i, j] == 4:
                receivers[cell] = (i + 1) * ncols + j
            elif arr[i, j] == 8:
                receivers[cell] = (i + 1) * ncols + j - 1
            elif arr[i, j] == 16:
                receivers[cell] = i * ncols + j - 1
            elif arr[i, j] == 32:
                receivers[cell] = (i - 1) * ncols + j - 1
            elif arr[i, j] == 64:
                receivers[cell] = (i - 1) * ncols + j
            elif arr[i, j] == 128:
                receivers[cell] = (i - 1) * ncols + j + 1
            else:
                raise ValueError(f"Invalid flow direction value: {arr[i, j]}")
    return receivers


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def count_donors(cnp.int64_t[:] r) -> int[:] :
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
def make_donor_array(cnp.int64_t[:] r, int[:] delta) -> int[:] :
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
def add_to_ordered_list(int l, int j, int[:] s, int[:] delta, int[:] donors) -> int:
    """
    Adds node l, and its donors (recursively), to the ordered list of nodes. Used in the recursive
    build_ordered_list function.

    Args:
        l: The node index.
        j: The list index.
        s: The ordered list.
        delta: The index array.
        donors: The donor information array.

    Returns:
        The updated ordered list index.
    """
    s[j] = l
    j += 1
    cdef int n, m
    for n in range(delta[l], delta[l + 1]):
        m = donors[n]
        if m != l:
            j = add_to_ordered_list(m, j, s, delta, donors)

    return j

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def build_ordered_list_recursive(cnp.int64_t[:] receivers, cnp.ndarray[cnp.int64_t, ndim=1] baselevel_nodes) -> int[:] :
    """
    Builds the ordered list of nodes in topological order, given the receiver array.
    Starts at the baselevel nodes and works upstream. This uses recursion 
    and as such is not recommended for large arrays. Included for legacy reasons. 
    Use build_ordered_list_iterative instead.

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
    cdef int[:] ordered_list = np.zeros(n, dtype=np.int32) - 1
    cdef int j = 0
    cdef int b
    for b in baselevel_nodes:
        j = add_to_ordered_list(b, j, ordered_list, delta, donors)
    return ordered_list      

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def build_ordered_list_iterative(cnp.int64_t[:] receivers, cnp.ndarray[cnp.int64_t, ndim=1] baselevel_nodes) -> int[:] :
    """
    Builds the ordered list of nodes in topological order, given the receiver array.
    Starts at the baselevel nodes and works upstream in a wave building a 
    breadth-first search order of the nodes using a queue. This is much faster
    than the recursive version. 

    Args:
        receivers: The receiver array (i.e., receiver[i] is the ID
        of the node that receives the flow from the i'th node).
        baselevel_nodes: The baselevel nodes to start from.

    Returns:
        The nodes in topological order (using a BFS).
    """
    cdef int n = len(receivers)
    cdef int[:] n_donors = count_donors(receivers)
    cdef int[:] delta = ndonors_to_delta(n_donors)
    cdef int[:] donors = make_donor_array(receivers, delta)
    cdef int[:] ordered_list = np.zeros(n, dtype=np.int32) - 1
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
        ordered_list[j] = node # Add the node to the stack
        j += 1 # Increment the stack index.
        # Loop through the donors of the node
        for n in range(delta[node], delta[node+1]):
            m = donors[n]
            if m != node:
                q[back] = m
                back += 1

    free(q)
    return ordered_list

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def accumulate_flow(
    cnp.int64_t[:] receivers, 
    int[:] ordered, 
    cnp.ndarray[double, ndim=1] weights
):
    """
    Accumulates flow along the stack of nodes in topological order, given the receiver array,
    the ordered list of nodes, and a weights array which contains the contribution from each node.

    Args:
        receivers: The receiver array (i.e., receiver[i] is the ID
        of the node that receives the flow from the i'th node).
        ordered: The ordered list of nodes.
        weights: The weights array (i.e., the contribution from each node).
    """
    cdef int n = receivers.shape[0]
    cdef cnp.ndarray[double, ndim=1] accum = weights.copy()
    cdef int i
    cdef cnp.int64_t donor, recvr

    # Accumulate flow along the stack from upstream to downstream
    for i in range(n - 1, -1, -1):
        donor = ordered[i]
        recvr = receivers[donor]
        if donor != recvr:
            accum[recvr] += accum[donor]

    return accum

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def get_channel_segments(
    cnp.int64_t[:] starting_nodes,
    int[:] delta,
    int[:] donors,
    double[:] field,
    float threshold= 0
):
    """
    Returns the channel segments for a D8 network, where each segment is a list of node indices. Only adds nodes to segments if some
    specified `field` value (e.g., upstream area) is greater or equal than the threshold. Each segment in the list of segments starts
    at a node and ends at a bifurcation or dead end. Each segment contains first the start node and then all nodes upstream of it
    in the order they are visited. The segments are ordered topologically, so that the first segment in the list
    is base-level, and the last segment in the list is an upstream tributary. Base level nodes present an edge case, and as such
    are always present *twice* in the list of segments. This prevents returning of segments containing *only* a single baselevel nodes.
    i.e., ensuring that every segment is a valid line (not a point).

    This function uses a stack (first in, first out) to keep track of nodes to visit. It also uses a stack to keep track of segments
    that are being built. This avoids recursion, which is slow. Its also written in (mostly) pure Cython, which is fast. 

    Args:
        starting_nodes: array of baselevel nodes that are used to start the segments (Expects these to exceed threshold)
        delta: array of delta values
        donors: array of donor nodes
        field: array of field values
        threshold: threshold value for field

    Returns:
        List of segments, where each segment is a list of node indices
    """
    # Create a vector of vector ints to store the segments    
    cdef vector[vector[int]] segments
    cdef stack[vector[int]] seg_stack # FIFO Stack of segments
    cdef vector[int] curr_seg # Temporary vector for storing segments
    cdef stack[int] s  # FIFO Stack of nodes to visit
    cdef int node # Current node
    cdef cnp.int64_t b # A baselevel node 
    cdef int n_donors # Number of donors for a node
    cdef int m # Donor node
    cdef int n # Donor index

    for b in starting_nodes:
        s.push(b) # Add the baselevel node to the stack
        curr_seg.clear() # Clear the current segment vector 
        curr_seg.push_back(b) # Add the baselevel node to the current segment vector
        seg_stack.push(curr_seg) # Add the current segment vector to the stack of segments
    if s.empty():
        # If there are no valid baselevel nodes, return an empty list
        return segments

    curr_seg = seg_stack.top()
    seg_stack.pop()
    while not s.empty():  
        node = s.top()
        s.pop()
        curr_seg.push_back(node)

        # Loop over donors
        n_donors = 0
        for n in range(delta[node], delta[node + 1]):
            m = donors[n]
            if m != node:
                # We don't want to add the node to the queue if it's the same as the current node
                if field[m] >= threshold:
                    # Only add the node to the queue if field > threshold.
                    s.push(m)
                    n_donors += 1
        if n_donors == 1:
            # We're still on the same segment, so we just continue...
            pass
        elif n_donors > 1:
            # We've reached a bifurcation! Add the current segment to the list of segments.
            segments.push_back(curr_seg)
            # Now we start a new segment for each donor, and put them in the segments queue.
            for _ in range(n_donors):
                curr_seg.clear()
                curr_seg.push_back(node)
                seg_stack.push(curr_seg)
            # Pop the last element of the segment stack and continue from where we left off.
            curr_seg.clear()
            curr_seg = seg_stack.top()
            seg_stack.pop()
        elif n_donors == 0:
            # We've reached a dead end! Add the current segment to the list of segments.
            segments.push_back(curr_seg)
            if seg_stack.empty():
                # If the segments queue is empty, we're done!
                break
            else:
                # Otherwise, pop the last element of the segment stack and continue from where we left off.
                curr_seg.clear()
                curr_seg = seg_stack.top()
                seg_stack.pop()
    return segments

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
def get_upstream_nodes(
    cnp.int64_t starting_node,
    int[:] delta,
    int[:] donors
):
    """
    Returns the nodes upstream of a starting node in a D8 network.

    Args:
        starting_node: The starting node.
        delta: The delta index array.
        donors: The donor array.

    Returns:
        A list of node IDs upstream of the starting node.
    """

    # Create a vector nts to store the output
    cdef vector[int] upstream_nodes
    cdef stack[int] s  # FIFO Stack of nodes to visit
    cdef int node # Current node
    cdef int m # Donor node
    cdef int n # Donor index

    s.push(starting_node) # Add the starting node to the stack
    while not s.empty():
        node = s.top()
        upstream_nodes.push_back(node)
        s.pop()
        # Loop over donors
        for n in range(delta[node], delta[node + 1]):
            m = donors[n]
            if m != node:
                s.push(m)
    return upstream_nodes

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cpdef (int, int) ItoXY(int i, int ncols):
    """
    Converts a node index for a 2D array to an x, y pair of col, row indices.

    Args:
        i: The node index.
        ncols: The number of columns in the array.

    Returns:
        The x, y coordinate pair.
    """
    cdef int x = i % ncols
    cdef int y = i // ncols
    return x, y

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cpdef vector[float] XYtocoords(int x, int y, float dx, float dy, float ULx, float ULy):
    """
    Converts a pair of col, row indices to a pair of geospatial x, y coordinates for the center of the cell.

    Args:
        x: The column index.
        y: The row index.
        dx: The cell size in the x direction.
        dy: The cell size in the y direction (assumed to be negative in keeping with geospatial convention)
        ULx: The x coordinate of the upper left corner of the array.
        ULy: The y coordinate of the upper left corner of the array.

    Returns:
        The x, y coordinate pair.
    """
    cdef vector[float] coords
    # Calculate the center coordinates of the cell (which is dx/2, dy/2 from the upper left corner)
    cdef float cx = (ULx + x * dx) + dx / 2
    cdef float cy = (ULy + y * dy) + dy / 2
    coords.push_back(cx)
    coords.push_back(cy)
    return coords

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.    
def id_segments_to_coords_segments(vector[vector[int]] segments, int ncols, float dx, float dy, float ULx, float ULy):
    """
    Converts a list of segments to a list of coordinates.

    Args: 
        ncols: The number of columns in the array.
        dx: The cell size in the x direction.
        dy: The cell size in the y direction (assumed to be negative in keeping with geospatial convention)
        ULx: The x coordinate of the upper left corner of the array.
        ULy: The y coordinate of the upper left corner of the array.

    Returns:
        A list of segments, where each segment is a list of coordinates (which are a list of two floats)
    """

    cdef vector[vector[vector[float]]] coords
    cdef vector[vector[float]] segment_coords
    cdef vector[float] coord
    cdef int x, y
    cdef int i, j
    cdef int nsegs = segments.size()
    cdef int npts
    cdef int node
    cdef vector[int] segment
    for i in range(nsegs):
        segment = segments[i]
        npts = segment.size()
        segment_coords.clear()
        for j in range(npts):
            node = segment[j]
            x, y = ItoXY(node, ncols)
            coord = XYtocoords(x, y, dx, dy, ULx, ULy)
            segment_coords.push_back(coord)
        coords.push_back(segment_coords)
    return coords

@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.    
def get_profile(cnp.int64_t start_node, float dx, float dy, cnp.int64_t[:] receivers, cnp.int64_t[:] d8):
    """
    Gets the profile of a channel segment, given the start node, the receiver array, and the D8 flow direction array. 

    Args:
        start_node: The node of the array to start the profile from.
        dx: The cell size in the x direction.
        dy: The cell size in the y direction.
        receivers: The array of receivers.
        d8: The D8 flow direction array. 
    """

    cdef vector[cnp.int64_t] profile 
    cdef vector[float] distance
    cdef float downstream_distance = 0
    downstream_distance = 0  # distance downstream from the start node
    current_node = start_node
    receiver = receivers[current_node]
    while current_node != receiver:
        profile.push_back(current_node)
        distance.push_back(downstream_distance)
        current_node = receivers[current_node]
        receiver = receivers[current_node]
        flow_dir = d8[current_node]
        if flow_dir == 1 or flow_dir == 16:
            # Flow going left or right
            downstream_distance += dx
        elif flow_dir == 4 or flow_dir == 64:
            # Flow going up or down
            downstream_distance += dy
        else:
            # Flow going diagonally
            downstream_distance += np.sqrt(dx**2 + dy**2)
    profile.push_back(current_node)
    distance.push_back(downstream_distance)

    return profile, distance
