from pyhop import pyhop
import socket
import time
import optparse
import sys
import multiprocessing as mp
from multiprocessing import Process

if len(sys.argv) < 3:
    print 'Usage: python %s -H Extrahop_IP -o OutputFile -d Lookback in days -k apikey' % (sys.argv[0])
    sys.exit(1)

    
#Setup options
p = optparse.OptionParser()
p.add_option("-H", "--host", dest="host", default="extrahop")
p.add_option("-K", "--key", dest="apikey", default="12345")
p.add_option("-o", "--file", dest="outputfile", default="default")
p.add_option("-d", "--days", dest="days", default="7")
(opts, argv) = p.parse_args()

# disable SSL CN monting
#import ssl
#ssl._create_default_https_context = ssl._create_unverified_context

#Connect to Extrahop
ehop = pyhop.Client(host=opts.host, apikey=opts.apikey)

#Name of the csv file we will be writing to....
f = open(opts.outputfile, "w")

#lookback
daysinMS = int(opts.days) * 86400000
lookback = 0 - daysinMS

#Write first line / headers of CSV file.
f.write("IPAddress, Hostname, Protocol, Peer IPaddress, Peer Hostname, Bytes In/Bytes Out, Bytes\n")

def grab_device_metrics(device):
    if(device.ipaddr4): # only care about L3 devices
        print "Grabbing Device " + str(device.ipaddr4)
        ipaddr = device.ipaddr4 #store ipaddress for later
        oid = device.oid #store device oid for later
        if(device.dns_name): #if we have a dns name... grab it
            host = device.dns_name
        else:
            host = "No DNS Name Captured" #if not... let the user know
        Failed = True #This script makes 'a lot' of calls... so if the Extrahop gets backed up... wait
        while(Failed):
            try:
                #grab Bytes in and bytes by L7 protocol
                metrics = ehop.get_exstats_total("extrahop.device.app_detail", "device", [(oid, lookback, 0)], ["bytes_in", "bytes_out"], {'cycle': "slow"})
                Failed = False
            except:
                #if the Extrahop is backed up.. wait 5 seconds and try again
                print "Extrahop backed up.. waiting 5 seconds"
                time.sleep(5)
        for stat in metrics.stats:
            for L7 in stat.bytes_out: #Loop thorugh L7 protocols
                for peer in L7.value: #Loop thorugh peer devices per L7 proto
                    try:
                        peerName = peer.key.host #check if we have a host value 
                    except:
                        peerName = "No DNS name gathered" #if not.. let the user know
                    #write to the screen and to the file
                    f.write(str(ipaddr) + "," + str(host) + "," + str(L7.key.str) + "," + str(peer.key.addr) + "," + str(peerName) + "," + "Bytes Out" + ","  + str(peer.value) + "\n")
#                    print str(ipaddr) + "," + str(host) + "," + str(L7.key.str) + "," + " " + str(peerName) + " " + str(peer.key.addr) + "," + str(peer.value)
            for L7 in stat.bytes_in: #Do same thing as above.. but for bytes in
                for peer in L7.value:
                    try:
                        peerName = peer.key.host
                    except:
                        peerName = "No DNS name gathered"
                    f.write(str(ipaddr) + "," + str(host) + "," + str(L7.key.str) + "," + str(peer.key.addr) + "," + str(peerName) + "," + "Bytes In" + "," +  str(peer.value) + "\n")
#                    print str(ipaddr) + "," + str(host) + "," + str(L7.key.str) + "," + " " + str(peerName) + " " + str(peer.key.addr) + "," + str(peer.value)
        return "Success for " + str(device.ipaddr4) + "\n"
if __name__ == '__main__':
    #Grab all of the Extrahop Devices
    devices = ehop.get_all_devices()
    jobs = []
    pool = mp.Pool(100)
    results = [pool.apply_async(grab_device_metrics, args=(device, )) for device in devices]
    output = [p.get() for p in results]
    f.close()
