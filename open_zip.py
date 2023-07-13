lst = ['sleepMode','covCellDlPrbWakeUpThreshold','capCellDlPrbSleepThreshold'
,'sleepStartTime'
,'sleepEndTime'
,'capCellRrcConnSleepThreshold'
,'covCellRrcConnWakeUpThreshold'
,'coverageCellDiscovery'
,'capCellSleepMonitorDurTimer'
,'covCellWakeUpMonitorDurTimer'
,'isAllowedMsmOnCovCell'
,'capCellSleepProhibitInterval'
,'covCellWakeUpMonitorDurTHigh'
,'sleepModeCovCellCandidate'
,'sleepModeCoverageCell'
,'sleepModeCapacityCell'
,'covCellDlPrbWakeUpThresHigh'
,'covCellRrcConnWakeUpThresHigh'
,'cellSleepCovCellMeasOn'
,'SleepState'
,'SleepProhibitStartTime'
,'sleepMode'
,'essEnabled'
,'essCellScPairs_gNBessLocalScId'
,'essCellScPairs_essScPairId'
,'essScPairId'
,'essScLocalId'
,'administrativeState'
,'administrativeState'
,'serviceState'
,'featureState'
,'licenseState'
,'description'
,'earfcndl'
,'earfcnul'
]


import lxml.etree as ET

filename = '/home/uventus/Works/NSN/celfinet_cm_20230619_062002.xml'
# this works, but loads the whole file into memory
parser = ET.XMLParser(recover=True) #recovers from bad characters.
tree = ET.parse(filename, parser)

tree.write('/home/uventus/Works/NSN/output.xml', pretty_print=True)