#light

open System
open System.Collections.Generic
open Clock
open Types
open Oper
open Extensions.DateTimeExtensions

type action = ChildData list * changes

type Scheduler =
    { clock:IClock
      queue:SortedList<DateTime, action list> }

let init () =
    { clock = VirtualClock ()
      queue = SortedList<DateTime, action list>() }

let sched = ref (init ())

let reset () = sched := init ()

let clock () = (!sched).clock

let rec reSchedule sched =
    let queue = sched.queue
    if queue.Count <> 0
        then sched.clock.Schedule(queue.Keys.[0], 
                                  fun _ -> // Sort the list of events to propagate according to node priority
                                           // and then spread them.
                                           let stack = List.fold (fun acc (children, changes) ->
                                                                    (List.fold (fun acc (childOp, idx, link) ->
                                                                                  mergeStack acc [childOp, ([idx, link changes])])
                                                                               acc children))
                                                                 [] queue.Values.[0]
                                           queue.RemoveAt 0
                                           spread stack
                                           reSchedule sched)
                                           
let schedule time action =
    let queue = (!sched).queue
    if queue.ContainsKey(time)
        then queue.[time] <- queue.[time] @ [action]
        else queue.[time] <- [action]
    reSchedule (!sched)

let scheduleOffset offset action =
    schedule ((!sched).clock.Now + TimeSpan(0, 0, offset)) action
    
let now () = clock().Now.TotalSeconds