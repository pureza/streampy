#light

open System
open System.Collections.Generic
open EzqlAst
open EzQL.Stream

type context = (string * value) list

and value =
    | Integer of int
    | Boolean of bool
    | Stream of EzQL.Stream.Stream<Dictionary<string, value>>
    | Closure of context * expr
    | Event of Dictionary<string, value>

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
        let event = Dictionary<string, value>()
        let result = Event event
        List.iter (fun (Symbol (name), expr) -> 
                    event.Add(name, eval env expr))
                  fields
        result                                
    | BinaryExpr (oper, expr1, expr2) -> 
        let value1 = eval env expr1
        let value2 = eval env expr2
        evalOp oper value1 value2
    | expr.Integer v -> value.Integer v
    | Id (Identifier name) -> lookup env name
    
and evalOp oper v1 v2 =
    match oper, v1, v2 with
    | GreaterThan, Integer v1, Integer v2 -> Boolean (v1 > v2)
    | Plus, Integer v1, Integer v2 -> Integer (v1 + v2)
    | _ -> failwithf "Wrong type oper = %A" oper

and evalMethod target name parameters =
    match target with
    | Stream stream ->
        let result =
            match name with
            | "where" -> evalWhere stream parameters
            | "select" -> evalSelect stream parameters
            | _ -> failwithf "Unknown method %s" name
        match result with
        | Stream stream -> stream.OnUpdate printEvent
        | _ -> failwith "Won't happen"
        result
    | _ -> failwith "This type has no methods?"

and evalWhere stream parameters =
    match parameters with
    | [(Closure (env, expr)) as closure] -> 
        Stream (where stream (fun ev -> match (evalClosure closure) [ev] with
                                        | Boolean b -> b
                                        | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"                                           

and evalSelect stream parameters =
    match parameters with
    | [(Closure (env, expr)) as closure] -> 
        Stream (select stream (fun ev -> match (evalClosure closure) [ev] with
                                         | Event v -> v.["timestamp"] <- ev.["timestamp"]
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

and printEvent (event: Dictionary<string, value>) =
    printf "{"
    for key in event.Keys do
        printf " %s: %A " key event.[key]
    printfn "}"