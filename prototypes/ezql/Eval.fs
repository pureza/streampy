﻿#light

open Extensions.DateTimeExtensions
open Ast
open Types

let rec eval env = function
  | FuncCall (Id (Identifier "print"), paramExps) -> // TODO: Put this in some sort of global environment
      let v = eval env paramExps.Head
      printfn "%O" v
      v
  | FuncCall (expr, paramExps) ->
      let fn = eval env expr
      let paramValues = List.map (eval env) paramExps
      apply fn paramValues
  | MemberAccess (expr, Identifier name) ->
      let target = eval env expr
      match target with
      | VEvent ev -> ev.[name]
      | VRecord r -> !r.[VString name]
      | _ -> failwith "eval MemberAccess: Not an event!"
  | ArrayIndex (target, index) ->
      let tv = eval env target
      let iv = eval env index
      match tv with
      | VDict dict -> if dict.ContainsKey(iv) then dict.[iv] else failwithf "Dictionary doesn't contain key %A. env = %A" iv env
      | _ -> failwithf "[]: Not a dictionary"
  | Record fields ->
      // This is extremely ineficient - we should create a node if possible
      VRecord (fields |> List.map (fun (Symbol (name), expr) -> (VString name, ref (eval env expr)))
                      |> Map.of_list)
  | RecordWith (source, newFields) ->
      match eval env source with
      | VRecord fields -> VRecord (List.fold_left (fun fields (Symbol (name), expr) -> fields.Add (VString name, ref (eval env expr))) fields newFields)
      | other -> failwithf "RecordWith: the source is not a record: %A" other
  | Lambda (args, body) as fn -> VClosure (env, fn)
  | Let (Identifier name, binder, body) ->
      eval (env.Add(name, eval env binder)) body
  | If (cond, thn, els) ->
      match eval env cond with
      | VBool true -> eval env thn
      | VBool false -> eval env els
      | x -> failwithf "The if condition is not a boolean: %A" x 
  | BinaryExpr (oper, expr1, expr2) ->
    let value1 = eval env expr1
    let value2 = eval env expr2
    evalOp (oper, value1, value2)
  | Seq (expr1, expr2) ->
      eval env expr1 |> ignore
      eval env expr2
  | Id (Identifier name) ->
      match Map.tryfind name env with
      | Some v -> v
      | _ -> failwithf "eval: Unknown variable or identifier: %s" name
  | Integer v -> VInt v
  | String v -> VString v
  | other -> failwithf "Not implemented: %A" other

and evalOp = function
  | Plus, v1, v2 -> value.Add(v1, v2)
  | Minus, v1, v2 -> value.Subtract(v1, v2)
  | Times, v1, v2 -> value.Multiply(v1, v2)
  | GreaterThan, v1, v2 -> value.GreaterThan(v1, v2)
  | LessThan, v1, v2 -> value.LessThan(v1, v2)
  | Equal, v1, v2 -> value.Equals(v1, v2)
  | _ -> failwith "op not implemented"


and apply = function
  | VClosure (env, expr) ->
      match expr with
      | Lambda (ids, body) ->
          (fun args ->
              let ids' = List.map (fun (Identifier name) -> name) ids
              let env' = List.fold_left (fun e (n, v) -> Map.add n v e)
                                         env (List.zip ids' args)
              eval env' body)
      | _ -> failwith "evalClosure: Wrong type"
  | _ -> failwith "This is not a closure"
