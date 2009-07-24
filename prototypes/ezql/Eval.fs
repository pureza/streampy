#light

open Extensions.DateTimeExtensions
open Ast
open Types
open Util
open Scheduler

let rec eval (env:Map<string, value>) = function
  | FuncCall (Id (Identifier "print"), paramExps) -> // TODO: Put this in some sort of global environment
      let v = eval env paramExps.Head
      printfn "%O" v
      v
  | FuncCall (Id (Identifier "$makeEnum"), paramExps) ->
      let label, paramValues = paramExps.[0], List.map (eval env) paramExps.Tail
      match label with
      | Id (Identifier label') -> VVariant (label', paramValues)
      | _ -> failwithf "Label is not a string."
  | FuncCall (expr, paramExps) ->
      let fn = eval env expr
      let paramValues = if paramExps.IsEmpty then [VUnit] else List.map (eval env) paramExps
      apply fn paramValues
  | MemberAccess (expr, Identifier "null?") ->
      match eval env expr with
      | VNull -> VBool true
      | _ -> VBool false
  | MemberAccess (expr, Identifier name) ->
      let target = eval env expr
      match target with
      | VNull -> VNull
      | VRecord r -> r.[VString name]
      | VInt i -> match name with
                  | "sec" -> target
                  | "min" -> VInt (i * 60)
                  | _ -> failwithf "eval: the type TyInt doesn't have field %s" name
      | other -> failwith "eval MemberAccess: Not an event: %A" other
  | ArrayIndex (target, index) ->
      let tv = eval env target
      let iv = eval env index
      match tv with
      | VDict dict -> if Map.contains iv dict then dict.[iv] else VNull //failwithf "Dictionary doesn't contain key %A. env = %A" iv env
      | VWindow contents ->
          match iv with
          | VInt i when i < 0 ->
              let i' = i + contents.Length
              if i' >= 0 && i' < contents.Length then contents.[i'] else VNull
          | VInt i -> if i < contents.Length then contents.[i] else VNull
          | _ -> failwithf "The index is not an integer, but a %A" iv
      | _ -> failwithf "[]: Not a dictionary"
  | Record fields ->
      // This is extremely ineficient - we should create a node if possible
      VRecord (fields |> List.map (fun (name, expr) -> (VString name, eval env expr))
                      |> Map.of_list)
  | RecordWith (source, newFields) ->
      match eval env source with
      | VRecord fields -> VRecord (List.fold (fun fields (name, expr) -> fields.Add (VString name, eval env expr)) fields newFields)
      | other -> failwithf "RecordWith: the source is not a record: %A" other
  | Lambda (args, body) as fn -> VClosure (env, fn, None)
  | Let (Identifier name, _, (Lambda _ as fn), body) ->
      eval (env.Add(name, VClosure (env, fn, Some name))) body
  | Let (Identifier name, _, binder, body) ->
      eval (env.Add(name, eval env binder)) body
  | If (cond, thn, els) ->
      match eval env cond with
      | VNull -> VNull
      | VBool true -> eval env thn
      | other -> eval env els
   | Match (expr, cases) ->
       match eval env expr with
       | VVariant (label, meta) ->
           let case = List.tryFind (fun (MatchCase (Identifier label', _, _)) -> label = label') cases
           let metaVar, body = match case with
                               | Some (MatchCase (_, metaVar, body)) -> metaVar, body
                               | None -> failwithf "Can't happen"
           match metaVar with
           | Some (Identifier metaVar') -> eval (env.Add(metaVar', meta.[0])) body
           | None -> eval env body
       | _ -> failwithf "Not a variant!"
  | BinaryExpr (oper, expr1, expr2) ->
    let value1 = eval env expr1
    let value2 = eval env expr2
    evalOp (oper, value1, value2)
  | Seq (expr1, expr2) ->
      eval env expr1 |> ignore
      eval env expr2
  | Id (Identifier name) ->
      match Map.tryFind name env with
      | Some v -> v
      | _ -> failwithf "eval: Unknown variable or identifier: %s" name
  | Integer v -> VInt v
  | Float f -> value.VFloat f
  | String v -> VString v
  | Bool v -> VBool v
  | Null -> VNull
  | other -> failwithf "Not implemented: %A" other

and evalOp = function
  | Plus,               v1, v2 -> value.Add(v1, v2)
  | Minus,              v1, v2 -> value.Subtract(v1, v2)
  | Times,              v1, v2 -> value.Multiply(v1, v2)
  | Div,                v1, v2 -> value.Div(v1, v2)
  | Mod,                v1, v2 -> value.Mod(v1, v2)
  | GreaterThan,        v1, v2 -> value.GreaterThan(v1, v2)
  | GreaterThanOrEqual, v1, v2 -> value.GreaterThanOrEqual(v1, v2)
  | LessThan,           v1, v2 -> value.LessThan(v1, v2)
  | LessThanOrEqual,    v1, v2 -> value.LessThanOrEqual(v1, v2)
  | Equal,              v1, v2 -> value.Equals(v1, v2)
  | NotEqual,           v1, v2 -> value.Differ(v1, v2)
  | And,                v1, v2 -> value.And(v1, v2)
  | Or,                 v1, v2 -> value.Or(v1, v2)

and apply value =
  match value with
  | VClosure (env', expr, itself) ->
      // If the closure is recursive, add itself to the environment
      let env = match itself with
                | Some name -> env'.Add(name, value)
                | _ -> env'
      match expr with
      | Lambda (ids, body) ->
          (fun args ->
              let ids' = List.map (fun (Param (Identifier name, _)) -> name) ids
              let env' = List.fold (fun e (n, v) -> Map.add n v e)
                                   env (List.zip ids' args)
              eval env' body)
      | _ -> failwith "evalClosure: Wrong type"
  | VClosureSpecial (_, expr, _, _, itself) -> apply (convertClosureSpecial value) // FIXME: Ugly stuff is ugly
  | _ -> failwith "This is not a closure"
