#light

open System
open System.Collections.Generic
open EzqlAst
open Types

let rec eval (env:context) = function
    | Assign (Identifier name, exp) -> let v = evalE env exp
                                       env.Add(name, v)

and evalE env = function
    | MethodCall (expr, (Identifier name), paramExps) ->
        let target = evalE env expr
        let paramVals = List.map (evalE env) paramExps
        evalMethod target name paramVals
    | FuncCall (expr, paramExps) ->
        let fn = evalE env expr
        let paramVals = List.map (evalE env) paramExps
        evalFunction fn paramVals        
    | MemberAccess (expr, Identifier name) ->
        let target = evalE env expr
        match target with
        | VEvent ev -> ev.[name]
        | _ -> failwith "eval MemberAccess: Not an event!"
    | Lambda (args, body) as fn -> VClosure (env, fn)
    | Record fields ->
        VRecord (Map.of_list (List.map (fun (Symbol (name), expr) ->
                                         (name, evalE env expr))
                                       fields))
    | ArrayIndex (target, index) ->
        let target' = evalE env target
        let index' = evalE env index
        match (target', index') with
        | (VStream istream, VTime (v, unit)) ->
            let rstream = Stream () :> IStream
            istream.OnAdd (fun ev -> Scheduler.scheduleOffset (toSeconds v unit)
                                                              (fun () -> rstream.Add(ev)))
            VStream (Window (istream, rstream))
        | (VContinuousValue cv, VTime (v, unit)) ->
            let rstream = Stream () :> IStream
            let window = ContValueWindow(cv.InsStream, rstream)
            (window :> IContValue).OnSet (fun (t, _) -> 
                                            if window.Previous.IsSome then 
                                                let prevT, prevV = window.Previous.Value
                                                Scheduler.scheduleOffset (toSeconds v unit)
                                                                         (fun () -> rstream.Add(Event.WithValue(prevT, prevV))))   
            
            VContinuousValue window
        | (VMap assoc, index) -> VContinuousValue assoc.[index]
        | _ -> failwith "eval ArrayIndex: Can only index streams."
    | BinaryExpr (oper, expr1, expr2) ->        
        let value1 = evalE env expr1
        let value2 = evalE env expr2
        evalOp oper value1 value2
    | expr.Time (exp, unit) ->
        match (evalE env exp) with
        | VInteger v -> VTime (v, unit)
        | _ -> failwith "Time expression must evaluate to an integer"
    | expr.Integer v -> VInteger v
    | SymbolExpr v -> VSym v
    | Id (Identifier name) ->
        if Map.mem name env
            then env.[name]
            else failwithf "eval: Unknown variable or identifier: %s" name

and evalOp oper v1 v2 =
    match oper, v1, v2 with
    | GreaterThan, VInteger v1', VInteger v2' -> VBoolean (v1' > v2')
    | GreaterThan, VContinuousValue v1', VInteger v2' -> VContinuousValue (v1'.op_GreaterThan(v2))
    | Plus, VInteger v1', VInteger v2' -> VInteger (v1' + v2')
    | Plus, VContinuousValue v1', _ -> VContinuousValue (v1'.op_Addition(v2))
    | Times, VInteger v1', VInteger v2' -> VInteger (v1' * v2')
    | _ -> failwithf "evalOp: Wrong type oper = %A" oper

and evalMethod target name parameters =
    match target with
    | VStream stream ->
        match name with
        | "where" -> evalStreamWhere stream parameters
        | "select" -> evalStreamSelect stream parameters
        | "groupby" -> evalStreamGroupBy stream parameters
        | "asContValue" -> evalAsContValue stream parameters
        | "sum" -> evalStreamSum stream parameters
        | _ -> failwithf "Unknown method %s" name
    | VContinuousValue cv ->
        match name with
        | "sum" -> evalSum cv parameters
        | "any" -> evalAny cv parameters
        | _ -> failwithf "Unknown method %s" name
    | VMap assoc ->
        match name with
        | "where" -> evalAssocWhere assoc parameters
        | _ -> failwith "Unknown method %s" name
    | _ -> failwith "This type has no methods?"

and evalStreamWhere stream parameters =
    match parameters with
    | [(VClosure (env, expr)) as closure] ->
        VStream (stream.Where(fun ev -> match (evalClosure closure) [VEvent ev] with
                                        | VBoolean b -> b
                                        | _ -> failwith "where: wrong result"))
    | _ -> failwith "where: Invalid parameter"

and evalStreamSelect stream parameters =
    match parameters with
    | [(VClosure (env, expr)) as closure] ->
        VStream (stream.Select(fun ev -> match (evalClosure closure) [VEvent ev] with
                                         | VRecord r -> r
                                         | _ -> failwith "select: wrong result"))
    | _ -> failwith "select: Invalid parameter"

and evalStreamGroupBy stream parameters =
    match parameters with
    | [VSym (Symbol field); (VClosure (env, expr)) as closure] ->
        VMap (stream.GroupBy(field, (fun group -> match (evalClosure closure) [VStream group] with
                                                  | VContinuousValue v -> v
                                                  | _ -> failwith "groupby: wrong result")))
    | _ -> failwith "groupby: Invalid parameters"

and evalAsContValue stream parameters =
    match parameters with
    | [VSym (Symbol field)] ->
        VContinuousValue (ContValue.FromStream (stream, field))
    | _ -> failwith "asContValue: Invalid parameter"

and evalAssocWhere assoc parameters =
    match parameters with
    | [(VClosure (env, expr)) as closure] ->
        VMap (assoc.Where(fun cv -> match (evalClosure closure) [VContinuousValue cv] with
                                    | VContinuousValue cv' -> cv'
                                    | _ -> failwith "where: wrong result"))
    | _ -> failwith "select: Invalid parameter"

and evalSum cv parameters =
    match parameters with
    | [] -> VContinuousValue (sum (cv.InsStream, cv.RemStream))
    | _ -> failwith "sum: Invalid parameter"

and evalAny cv parameters =
    match parameters with
    | [] -> VContinuousValue (any (cv.InsStream, cv.RemStream))
    | _ -> failwith "sum: Invalid parameter"
    
and evalStreamSum stream parameters =
    match parameters with
    | [VSym (Symbol field)] ->
        let mappedStream = stream.Select (fun ev -> Map.of_list [("value", ev.[field])])
        VContinuousValue (sum (mappedStream.InsStream, mappedStream.RemStream))
    | _ -> failwith "sum: Invalid parameter"
   
and evalClosure = function
    | VClosure (env, expr) ->
        match expr with
        | Lambda (ids, body) ->
            (fun args ->
                let ids' = List.map (fun (Identifier name) -> name) ids
                let env' = List.fold_left (fun (e:context) (n, v) -> e.Add(n, v)) 
                                          env (List.zip ids' args)
                evalE env' body)
        | _ -> failwith "evalClosure: Wrong type"
    | _ -> failwith "This is not a closure"

and evalFunction fn paramVals =
    match fn with
    | VType name when name = "stream" -> VStream (Stream ())
    | _ -> failwith "Calling functions not yet implemented"
   
and sum(addStream:IStream, expireStream:IStream) =
    let insStream = Stream () :> IStream
    let cv = ContValue(insStream, VInteger 0) :> IContValue
   
    addStream.OnAdd (fun ev -> insStream.Add(Event.WithValue(ev.Timestamp, cv.Current + ev.["value"])))
    expireStream.OnAdd (fun ev -> 
        let now = (Scheduler.clock ()).Now
        insStream.Add(Event.WithValue(now, cv.Current - ev.["value"])))
    cv

and count(addStream:IStream, expireStream:IStream) =
    let insStream = Stream () :> IStream
    let cv = ContValue(insStream, VInteger 0) :> IContValue
   
    addStream.OnAdd (fun ev -> insStream.Add(Event.WithValue(ev.Timestamp, cv.Current + VInteger 1)))
    expireStream.OnAdd (fun ev -> 
        let now = (Scheduler.clock ()).Now
        insStream.Add(Event.WithValue(now, cv.Current - VInteger 1)))
    cv

and any(addStream:IStream, expireStream:IStream) =
    let insStream = Stream () :> IStream
    let cv = ContValue(insStream, VBoolean false) :> IContValue
    let trueCount = ref 0
   
    addStream.OnAdd (fun ev -> 
                        let value = ev.["value"]
                        match value with
                        | VBoolean true -> trueCount := !trueCount + 1
                        | _ -> ()
                        if cv.Current = VBoolean false && !trueCount > 0 then
                            insStream.Add(Event.WithValue(ev.Timestamp, VBoolean true)))
                            
    expireStream.OnAdd (fun ev -> 
                          let value = ev.["value"]
                          match value with
                          | VBoolean true -> trueCount := max (!trueCount - 1) 0
                          | _ -> ()
                          if cv.Current = VBoolean true && !trueCount = 0 then
                              let now = (Scheduler.clock ()).Now
                              insStream.Add(Event.WithValue(now, VBoolean false)))

    cv  
      