Discovery Module
================
This module provides the model and default implementations to support discovery, identification, synchronization,
and connection (link) resolution of network elements under the OpenDaylight (ODL) controller. The definition of these
capabilities for the purpose of this module are:

* __discovery__ - inform the lifecycle processing of the existance of a network element
* __identification__ - a discovered network element is associated with an OpenDaylight (ODL) node type, such that RPCs
routed based on this node type will provide node type specific implementations.
* __synchronization__ - an identified network element's configuration is resolved or synchronized into the OpenDaylight
information model
* __connection resolution__ - connections or links between synchronized network elements are identified and stored into
the OpenDaylight information model

General Flow
------------
The general flow of the discovery process is that when a network element is discovered a notification is published
that is "handled" by each network element support module. The module attempts to connect to the network element and if
it is successful it publishes a network element identified notification indicating the node type to be associated with
the network element.

The notifications of identified network elements are handled by a synchronization module which queues up a request to
synchronize the network element. These requests are executed against a thread pool. When the synchronization request
is activated a routed RPC, "synchronize" is called against the network element. This request is handled by the
network element support module and should perform a deep inspection of the device and update the OpenDaylight
inventory appropriately. When a synchronization is complete the synchronization module will publish a synchronized
notification.

The synchronized notification will be handled by the connection resolution module. This module will evaluate the known
information about the network and based on any additional information about a network element that was synchronized it
may create, delete, or update the network element connections in the OpenDaylight information model.

