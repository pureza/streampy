#light

open Ast
open Types

let rec eval env = function
  | FuncCall (Id (Identifier "print"), paramExps) ->
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
      | _ -> failwith "eval MemberAccess: Not an event!"
  | Record fields ->
      // This is extremely ineficient - we should create a node if possible
      VRecord (fields |> List.map (fun (Symbol (name), expr) -> (VString name, ref (eval env expr)))
                      |> Map.of_list)
  | Lambda (args, body) as fn -> VClosure (env, fn)
  | Let (Identifier name, binder, body) ->
      eval (env.Add(name, eval env binder)) body
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
  | expr.Integer v -> VInt v
  | other -> failwithf "Not implemented: %A" other

and evalOp = function
  | Plus, v1, v2 -> value.Add(v1, v2)
  | Minus, v1, v2 -> value.Subtract(v1, v2)
  | Times, v1, v2 -> value.Multiply(v1, v2)
  | GreaterThan, v1, v2 -> value.GreaterThan(v1, v2)
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
(*

and evalClosure = function
    | VClosure (env, expr) ->
        match expr with
        | Lambda (ids, body) ->
            (fun args ->
                let ids' = List.map (fun (Identifier name) -> name) ids
                let env' = List.fold_left (fun e (n, v) -> Map.add n v e)
                                           env (List.zip ids' (List.map ref args))
                eval env' body)
        | _ -> failwith "evalClosure: Wrong type"
    | _ -> failwith "This is not a closure"

    *)