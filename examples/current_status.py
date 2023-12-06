"""
This notebook provides an example of how to use the `poopy` package to access live and historical event duration monitoring data provided by English water companies.

First, we import the libraries we need.
"""
from poopy.companies import ThamesWater

# This is just to demonstrate the time varying nature of the data!
import time

# The intended way to access active EDM data is by instantiating a
# `WaterCompany` object, which corresponds to the EDM sensor network
# maintained by a specific water comapny. When initialising the object,
#  it is populated with all of the active (i.e., _transmitting_) EDM
#  monitors maintained by Thames Water. However, we do not create a
# `WaterCompany` object directly. Instead, each water company defines a
#  sub-class of a `WaterCompany` object. This is because each company
# transmits their data via different APIs and so the data is accessed
# in slightly different ways. However, this is all done 'behind the
# scenes', so they are all interacted with in the exact same way.
# The only difference is the name of the class. Lets have a look
# at the data in Thames Water's active EDM monitors... This requires
# us to specify access codes for the API which you can obtain
# [here](https://data.thameswater.co.uk/s/)

tw_clientID = "REPLACE_WITH_YOUR_ID"
tw_clientSecret = "REPLACE_WITH_YOUR_SECRET"

if (
    tw_clientID == "REPLACE_WITH_YOUR_ID"
    or tw_clientSecret == "REPLACE_WITH_YOUR_SECRET"
):
    raise ValueError(
        "You need to set your Thames Water client ID and client secret in the script!"
    )

tw = ThamesWater(tw_clientID, tw_clientSecret)

# We can see the names of these monitors by using the `active_monitor_names`
# attribute

print("-" * 50)
print("Number of active monitors: ", len(tw.active_monitor_names))
print("Active monitor names: ")
for name in tw.active_monitor_names:
    print("\t", name)
print("-" * 50)

# Lets see the current status of a random monitor in the network. Lets
#  extract the [42]nd monitor in the network

print("Selecting an arbitrary monitor...")
name = tw.active_monitor_names[42]
print("Monitor name: ", name)

# The monitors are stored in a `Dictionary` of `Monitor` objects, which
# can be accessed using the `monitors` attribute. We now extract the
# `Monitor` object corresponding to the name we have just extracted.
# We can then use the `print_status` method to print the current status
# of the monitor.

monitor = tw.active_monitors[name]
monitor.print_status()

# Each `Monitor` stores the `WaterCompany` object which contains the
# monitor. For example, the `Monitor` we have just extracted is maintained
# by the following `WaterCompany` object:

print("Monitor maintained by: ", monitor.water_company.name)

# We can also see when the information was last updated by querying
# the WaterCompany object's timestamp attribute

print("Monitor data last updated: ", monitor.water_company.timestamp)

# Lets say we think maybe there has been a change in the status of the
#  monitor since the last update. We can use the `WaterCompany`'s
# `update()` method to update the status of the `Monitor`. Note that
# this updates all `Monitor`s maintained by the `WaterCompany` object,
# not just the one we are interested in.

print("... pausing for 5 seconds ...")
time.sleep(5)
print("Updating monitor data...")
monitor.water_company.update()
print("Monitor last updated: ", monitor.water_company.timestamp)

# Note that the timestamp has been updated.

# The `current_event` attribute of the `Monitor` object stores an `Event`
# object that contains specific information. `Event` is a class that
# contains three sub-classes corresponding to the three different types
# of status that can be recorded: `Discharge`, `NoDischarge` and `Offline`. The `current_event` attribute will contain an object of one of these three classes, depending on the current status of the monitor.
# Lets see what the current status of the event recorded at the monitor is:

print("Extracting current event at monitor...")
event = monitor.current_event
print("Event ongoing? ", event.ongoing)

# Because the event is ongoing it doesn't have an end time, but it does
# have a start time. As a result, the `duration` attribute of the `Event`
# object updates dynamically to show the duration of the event so far
# (in minutes). We can see this here.

print("Event start:", event.start_time)
print("Event duration:", event.duration, "minutes")
print("... pausing for 5 seconds ...")
time.sleep(5)
print("Event duration (5 seconds later):", event.duration, "minutes")

# Lets query all the CSOs that are currently discharging using the
# `WaterCompany`'s `discharging_monitors` attribute. This returns a
# list of `Monitor` objects that are currently discharging. We loop
# through and print their summaries. Note how the colour of the summary
# changes depending on the status of the monitor.

print("Extracting all discharging CSOs...")
discharging = tw.discharging_monitors
print("Printing summary of all discharging CSOs...")
for monitor in discharging:
    monitor.print_status()