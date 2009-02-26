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
        | _ -> failwith "Not an event!"
    | Lambda (args, body) as fn -> VClosure (env, fn)
    | Record fields ->
        VRecord (Map.of_list (List.map (fun (Symbol (name), expr) ->
                                         (name, evalE env expr))
                                       fields))
    | ArrayIndex (target, index) ->
        let target' = evalE env target
        let index' = evalE env index
        match (target', index') with
        | (VStream stream, VTime (v, unit)) -> VStream (Window (toSeconds v unit, stream))
        | (VContinuousValue cv, VTime (v, unit)) -> VContinuousValue (ContValueWindow.FromContValue(cv, (toSeconds v unit)))
        | _ -> failwith "Can only index streams"
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
        env.[name]

and evalOp oper v1 v2 =
    match oper, v1, v2 with
    | GreaterThan, VInteger v1, VInteger v2 -> VBoolean (v1 > v2)
    | Plus, VInteger v1, VInteger v2 -> VInteger (v1 + v2)
    | Times, VInteger v1, VInteger v2 -> VInteger (v1 * v2)
    | _ -> failwithf "Wrong type oper = %A" oper

and evalMethod target name parameters =
    match target with
    | VStream stream ->
        match name with
        | "where" -> evalStreamWhere stream parameters
        | "select" -> evalStreamSelect stream parameters
        | "asContValue" -> evalAsContValue stream parameters
        | "sum" -> evalStreamSum stream parameters
        | _ -> failwithf "Unknown method %s" name
    | VContinuousValue cv ->
        match name with
        | "sum" -> evalSum cv parameters    
        | _ -> failwithf "Unknown method %s" name
    | _ -> failwith "This type has no methods?"

and evalStreamWhere stream parameters =
    match parameters with
    | [(VClosure (env, expr)) as closure] ->
        VStream (stream.Where(fun ev -> match (evalClosure closure) [ev] with
                                        | VBoolean b -> b
                                        | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"

and evalStreamSelect stream parameters =
    match parameters with
    | [(VClosure (env, expr)) as closure] ->
        VStream (stream.Select(fun ev -> match (evalClosure closure) [ev] with
                                         | VRecord r -> r
                                         | _ -> failwith "wrong"))
    | _ -> failwith "Invalid parameter"

and evalAsContValue stream parameters =
    match parameters with
    | [VSym (Symbol field)] ->
        VContinuousValue (ContValue.FromStream (stream, field))
    | _ -> failwith "Invalid parameter"


and evalSum cv parameters =
    match parameters with
    | [] -> VContinuousValue (ContValueSum.FromContValue(cv))
    | _ -> failwith "Invalid parameter"

and evalStreamSum stream parameters =
    match parameters with
    | [VSym (Symbol field)] ->
        VContinuousValue (ContValueSum.FromStream (stream, field))
    | _ -> failwith "Invalid parameter"
    
and evalClosure = function
    | VClosure (env, expr) ->
        match expr with
        | Lambda (ids, body) ->
            (fun args ->
                let ids' = List.map (fun (Identifier name) -> name) ids
                let args' = List.map VEvent args
                let env' = List.fold_left (fun (e:context) (n, v) -> e.Add(n, v)) 
                                          env (List.zip ids' args')
                evalE env' body)
        | _ -> failwith "Wrong type"
    | _ -> failwith "This is not a closure"

and evalFunction fn paramVals =
    match fn with
    | VType name when name = "stream" -> VStream (Stream ())
    | _ -> failwith "Calling functions not yet implemented"
    