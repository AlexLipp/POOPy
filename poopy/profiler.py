# coding: utf8
# ! /usr/env/python
"""channel_profiler.py component to create channel profiles. Modified from 
base python component to simplify and speed up."""
from collections import OrderedDict
from typing import Tuple

import numpy as np
from landlab import RasterModelGrid
from landlab.components.profiler.base_profiler import _BaseProfiler
from landlab.core.utils import as_id_array


class ChannelProfiler(_BaseProfiler):
    _name = "ChannelProfiler"

    _info = {
        "flow__link_to_receiver_node": {
            "dtype": int,
            "intent": "in",
            "optional": False,
            "units": "-",
            "mapping": "node",
            "doc": "ID of link downstream of each node, which carries the discharge",
        },
        "flow__receiver_node": {
            "dtype": int,
            "intent": "in",
            "optional": False,
            "units": "-",
            "mapping": "node",
            "doc": "Node array of receivers (node that receives flow from current node)",
        },
    }
    """Class to extract all nodes in a drainage network that exceed some arbitrary threshold  """

    def __init__(
        self,
        grid: RasterModelGrid,
        channel_definition_field: str,
        minimum_outlet_threshold: float = 0,
        minimum_channel_threshold: float = 0,
    ):
        """
        Parameters
        ----------
        grid : Landlab Model Grid instance
        channel_definition_field : field name as string, optional
            Name of field used to identify the outlet and headwater nodes of the
            channel network. e.g., "drainage_area".
        minimum_outlet_threshold : float, optional
            Minimum value of the *channel_definition_field* to define a
            watershed outlet. Default is 0.
        minimum_channel_threshold : float, optional
            Value to use for the minimum drainage area associated with a
            plotted channel segment. Default values 0.
        """
        super().__init__(grid)
        if channel_definition_field in grid.at_node:
            self._channel_definition_field = grid.at_node[channel_definition_field]
        else:
            msg = "Required field {name} not present. This field is required by the ChannelProfiler to define the start and stop of channel networks.".format(
                name=channel_definition_field
            )
            raise ValueError(msg)
        # Identify nodes which are sinks
        sink_nodes = grid.nodes.flatten()[grid.at_node["flow__sink_flag"]]
        self._flow_receiver = grid.at_node["flow__receiver_node"]
        self._link_to_flow_receiver = grid.at_node["flow__link_to_receiver_node"]
        self._minimum_channel_threshold = minimum_channel_threshold

        large_outlet_ids = sink_nodes[np.argsort(self._channel_definition_field[sink_nodes])]
        big_enough_watersheds = self._channel_definition_field[large_outlet_ids] >= max(
            minimum_outlet_threshold, minimum_channel_threshold
        )
        outlet_nodes = large_outlet_ids[big_enough_watersheds]

        starting_vals = self._channel_definition_field[outlet_nodes]
        outlet_nodes = np.asarray(outlet_nodes)

        bad_wshed = False
        if outlet_nodes.size == 0:
            bad_wshed = True  # not tested
        if np.any(starting_vals < minimum_outlet_threshold):
            bad_wshed = True
        if np.any(starting_vals < minimum_channel_threshold):
            bad_wshed = True

        if bad_wshed:
            raise ValueError(
                "The number of watersheds requested by the ChannelProfiler is "
                "greater than the number in the domain with channel_definition_field"
                f" area. {starting_vals}"
            )

        self._outlet_nodes = outlet_nodes

    @property
    def data_structure(self):
        """OrderedDict defining the channel network.

        The IDs and upstream distance of the channel network nodes are stored
        in ``data_structure``. It is a dictionary with keys of the outlet node
        ID.

        For each watershed outlet, the value in the ``data_structure`` is
        itself a dictionary with keys that are a segment ID tuple of the
        ``(dowstream, upstream)`` nodes IDs of each channel segment.

        The value associated with the segment ID tuple
        ``(dowstream, upstream)`` is itself a dictionary. It has three
        key-value pairs. First, ``"ids"`` contains a list of the segment node
        IDs ordered from downstream to upstream. It includes the endpoints.
        Second, ``"distances"`` contains a list of distances upstream that
        mirrors the list in ``"ids"``. Finally, ``"color"`` is an RGBA tuple
        indicating the color for the segment.
        """
        return self._data_struct

    def _get_channel_segment(self, i: int) -> Tuple[list, list]:
        """Get channel segment and return additional nodes to process.

        Parameters
        ----------
        i : int, required
            Node id of start of channel segment.

        Returns
        ----------
        channel_segment : list
            Node IDs of the nodes in the current channel segment.
        nodes_to_process, list
            List of nodes to add to the processing queue. These nodes are those
            that drain to the upper end of this channel segment. If
            main_channel_only = False this will be an empty list.
        """
        j = i
        channel_segment = []
        channel_upstream = True

        # add the reciever of j to the channel segment if it is not j.
        # but only do this when j is not the watershed outlet.
        recieving_node = self._flow_receiver[j]
        if (recieving_node != j) and (j not in self._outlet_nodes):
            channel_segment.append(recieving_node)

        while channel_upstream:

            # add the new node to the channel segment
            channel_segment.append(j)

            # get supplying nodes
            supplying_nodes = np.where(self._flow_receiver == j)[0]

            # remove supplying nodes that are the outlet node
            supplying_nodes = supplying_nodes[np.where(supplying_nodes != i)]

            # if only adding the biggest channel, continue upstream choosing the
            # largest node until no more nodes remain.

            # get all upstream channel properties
            upstream_vals = self._channel_definition_field[supplying_nodes]
            # if no nodes upstream exceed the threshold, exit
            if np.sum(upstream_vals > self._minimum_channel_threshold) == 0:
                nodes_to_process = []
                channel_upstream = False

            # otherwise
            else:
                # if only one upstream node exceeds the threshold, proceed
                # up the channel.
                if np.sum(upstream_vals > self._minimum_channel_threshold) == 1:
                    max_drainage = np.argmax(self._channel_definition_field[supplying_nodes])
                    j = supplying_nodes[max_drainage]
                # otherwise provide the multiple upstream nodes to be
                # processed into a new channel.
                else:
                    nodes_to_process = supplying_nodes[
                        upstream_vals > self._minimum_channel_threshold
                    ]
                    channel_upstream = False

        return (channel_segment, nodes_to_process)

    def _create_profile_structure(self):
        """Create the profile_IDs data structure for channel network.

        The bound attribute self._profile structure is the channel segment
        datastructure. profile structure is a list of length
        number_of_watersheds. Each element of profile_structure is itself a
        list of length number of stream segments that drain to each of the
        starting nodes. Each stream segment list contains the node ids of a
        stream segment from downstream to upstream.
        """
        self._data_struct = OrderedDict()
        for i in self._outlet_nodes:
            channel_network = OrderedDict()
            queue = [i]
            while len(queue) > 0:
                node_to_process = queue.pop(0)
                (channel_segment, nodes_to_process) = self._get_channel_segment(node_to_process)
                segment_tuple = (channel_segment[0], channel_segment[-1])
                channel_network[segment_tuple] = {"ids": as_id_array(channel_segment)}
                queue.extend(nodes_to_process)
            self._data_struct[i] = channel_network

        self._create_flat_structures()

    def _create_flat_structures(self):
        """Create expected flattened structures for ids"""
        self._nodes = []
        for outlet_id in self._data_struct:
            seg_tuples = self._data_struct[outlet_id].keys()
            self._nodes.extend([self._data_struct[outlet_id][seg]["ids"] for seg in seg_tuples])