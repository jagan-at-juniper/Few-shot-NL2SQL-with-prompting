import subprocess

MAX_CHARS = 100_000


def validate(trace):
    if not isinstance(trace, str):
        return False
    if len(trace) > MAX_CHARS:
        return False
    return True


def demangle(trace):
    demangled = ""
    if validate(trace):
        try:
            inputs = ["echo", trace]
            echo = subprocess.Popen(inputs,
                                    stdout=subprocess.PIPE)
            demangle_proc = subprocess.Popen(["c++filt", "-n"],
                                             stdin=echo.stdout,
                                             stdout=subprocess.PIPE)
            demangled, _ = demangle_proc.communicate()
            _ = echo.communicate()

        except:
            print("Error in demangling stack trace")

    return demangled.decode("utf-8").strip() if demangled else None


if __name__ == '__main__':
    stacktrace = """
PID: 13613 (highway)
UID: 0 (root)
GID: 0 (root)
Signal: 11 (SEGV)
Timestamp: Fri 2020-08-14 20:11:19 UTC (2 days ago)
Command Line: /usr/bin/highway --startup-log-level Info
Executable: /usr/bin/highway
Control Group: /system.slice/128T.service
Unit: 128T.service
Slice: system.slice
Boot ID: 6d7564bb3b074320a1f48038c8e0c794
Machine ID: d551f2a52410441a8cf51c2ced00a272
Hostname: rsri2-128thead02p4
Coredump: /var/lib/systemd/coredump/core.highway.0.6d7564bb3b074320a1f48038c8e0c794.13613.1597435879000000.xz
Message: Process 13613 (highway) of user 0 dumped core.

Stack trace of thread 13796:
#0  0x0000000000661606 _ZNSt16_Sp_counted_baseILN9__gnu_cxx12_Lock_policyE2EE10_M_releaseEv (highway)
#1  0x0000000000659037 _ZNSt14__shared_countILN9__gnu_cxx12_Lock_policyE2EEaSERKS2_ (highway)
#2  0x00007f8487acbd11 _ZN7routing22ServiceInformationBase31refreshLoadBalancedServicePathsERKSt10shared_ptrIN7highway7ServiceEERKS1_INS2_8balancer24LoadBalancedServicePathsEE (lib128T_routingAgent.so)
#3  0x00007f8487acbc3c _ZN7routing22ServiceInformationBase31refreshLoadBalancedServicePathsERKSt10shared_ptrIN7highway7ServiceEERSt6vectorIS1_INS2_8balancer24LoadBalancedServicePathsEESaISA_EESD_ (lib128T_routingAgent.so)
#4  0x00007f8487ad3326 _ZN7highway27ServiceRefreshActionBuilder23updateLoadBalancedPathsEv (lib128T_routingAgent.so)
#5  0x00007f8487ad328b _ZN7highway27ServiceRefreshActionBuilder7refreshEv (lib128T_routingAgent.so)
#6  0x00007f8487ad30ab _ZN7highway21ServiceRefreshHandler19processBulkTimeoutsEv (lib128T_routingAgent.so)
#7  0x00007f8487ba0520 _ZN3app18BulkTimeoutHandlerIN7highway7ServiceELNS_21ThreadingPolicyTraits15ThreadingPolicyE1EE14processTimeoutEv (lib128T_routingAgent.so)
#8  0x00007f84a296bd77 _ZN4util12TimeoutQueue33executeTimeoutInPollerThreadMutexERNS_7TObjectE (lib128T_util.so)
#9  0x00007f84a296c37a _ZN4util12TimeoutQueue28executeTimeoutInPollerThreadEmRNS_7TObjectE (lib128T_util.so)
#10 0x00007f84a296c5ae _ZN4util12TimeoutQueue15processTimeoutsEv (lib128T_util.so)
#11 0x00007f84a296ce5c _ZN4util12PollerThread3runEv (lib128T_util.so)
#12 0x00007f84874467c0 _ZN3app22NonSignaledApplication3runEv (lib128T_app.so)
#13 0x00007f8484704c4d n/a (lib128T_highwayManager.so)
#14 0x000000000053de0f _ZNSt13__future_base13_State_baseV29_M_do_setEPSt8functionIFSt10unique_ptrINS_12_Result_baseENS3_8_DeleterEEvEEPb (highway)
#15 0x00007f849f28e227 __pthread_once_slow (libpthread.so.0)
#16 0x00007f848470fadc n/a (lib128T_highwayManager.so)
#17 0x00007f84846fd35e n/a (lib128T_highwayManager.so)
#18 0x000000000053e1d3 _ZN4util10ThreadPool6Thread3runEv (highway)
#19 0x00007f847f4a8baf n/a (libstdc++.so.6)
#20 0x00007f849f2864f9 start_thread (libpthread.so.0)
#21 0x00007f847ebc739f __clone (libc.so.6)"""
    demangled_trace = demangle(stacktrace)
    print(len(demangled_trace), demangled_trace)

