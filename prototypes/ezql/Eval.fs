#light

open System
open System.Collections.Generic
open EzqlAst
open Stream
open Types

let rec lookup context name =
    match context with
    | [] -> failwithf "%s not found in context %A" name context
    | ((name', value)::xs) when name' = name -> value
    | (x::xs) -> lookup xs name

let rec eval env = function
    | Assign (Identifier name, exp) -> let v = eval env exp
                                       //env <- env.Add(name, v)
                                       v
    | MethodCall (expr, (Identifier name), paramExps) ->
        let target = eval env expr
        let paramVals = List.map (eval env) paramExps
        evalMethod target name paramVals
    | MemberAccess (expr, Identifier name) ->
        let target = eval env expr
        match target with
        | Event ev -> ev.[name]
        | _ -> failwith "Not an event!"
    | Lambda (args, body) as fn -> Closure (env, fn)
    | Record fields ->
        let event = new Event () :> IEvent
        let result = Event event
        List.iter (fun (Symbol (name), expr) ->
                    event.[name] <- eval env expr)
                  fields
        result
    | ArrayIndex (target, index) ->
        let target' = eval env target
        let index' = eval env index
        match (target', index') with
        | (Stream stream, Time (v, unit)) -> value.Window (Window.FromStream(stream, (toSeconds v unit)))
        | _ -> failwith "Can only index streams"
    | BinaryExpr (oper, expr1, expr2) ->
        let value1 = eval env expr1
        let value2 = eval env expr2
        evalOp oper value1 value2
    | expr.Time (exp, unit) ->
        match (eval env exp) with
        | Integer v -> value.Time (v, unit)
        | _ -> failwith "Time expression must evaluate to an integer"
    | expr.Integer v -> value.Integer v
    | SymbolExpr v -> Sym v
    | Id (Identifier name) -> lookup env name

and evalOp oper v1 v2 =
    match oper, v1, v2 with
    | GreaterThan, Integer v1, Integer v2 -> Boolean (v1 > v2)
    | Plus, Integer v1, Integer v2 -> Integer (v1 + v2)
    | Times, Integer v1, Integer v2 -> Integer (v1 * v2)
    | _ -> failwithf "Wrong type oper = %A" oper

and evalMethod target name parameters =
    match target with
    | Stream stream ->
        let result =
            match name with
            | "where" -> evalStreamWhere stream parameters
            | "select" -> evalStreamSelect stream parameters
            | _ -> failwithf "Unknown method %s" name
//        match result with
//        | Stream stream' -> stream' |> Stream.print
//        | _ -> failwith "Won't happen"
        result
    | Window window ->
        let result =
            match name with
            | "where" -> evalWindowWhere window parameters
            | "select" -> evalWindowSelect window parameters
            | _ -> failwithf "Unknown method %s" name
//        match result with
//        | Window window' -> window' |> Stream.printWindow
//        | _ -> failwith "Won't happen"
        result
    | _ -> failwith "This type has no methods?"

and evalStreamWhere stream parameters =
    match parameters with
    | [(Closure (env, expr)) as closure] ->
        Stream (stream.Where(fun ev -> match (evalClosure closure) [ev] with
                                       | Boolean b -> b
                                       | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"

and evalStreamSelect stream parameters =
    match parameters with
    | [(Closure (env, expr)) as closure] ->
        Stream (stream.Select(fun ev -> match (evalClosure closure) [ev] with
                                        | Event v -> v.Timestamp <- ev.Timestamp
                                                     v
                                        | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"

and evalWindowWhere window parameters =
    match parameters with
    | [(Closure (env, expr)) as closure] ->
        Window (window.Where(fun ev -> match (evalClosure closure) [ev] with
                                       | Boolean b -> b
                                       | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"

and evalWindowSelect window parameters =
    match parameters with
    | [(Closure (env, expr)) as closure] ->
        Window (window.Select(fun ev -> match (evalClosure closure) [ev] with
                                        | Event v -> v.Timestamp <- ev.Timestamp
                                                     v
                                        | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"

and evalClosure = function
    | Closure (env, expr) ->
        match expr with
        | Lambda (ids, body) ->
            (fun args ->
                let ids' = List.map (fun (Identifier name) -> name) ids
                let args' = List.map Event args
                let env' = (List.zip ids' args')@env
                eval env' body)
        | _ -> failwith "Wrong type"
    | _ -> failwith "This is not a closure"
