#light

open Ast


type Type =
  | Function of string * Lazy<Type> list * Lazy<Type>
  | Class of string * TypeContext
  | Event
  | Bool
  | Int
  | Time
  | Symbol
  | Any
  
  override self.ToString() =
    match self with
    | Function (name, _, _) -> name + "()"
    | Class (name, _) -> "class " + name
    | Event -> "event"
    | Bool -> "bool"
    | Int -> "int"
    | Time -> "time"
    | Symbol -> "symbol"
    | Any -> "any"


and TypeContext = Map<string, Type>

let rec types (env:TypeContext) = function
  | Assign (Identifier name, expr) ->
      let typ = typeOf env expr
      env.Add(name, typ)

and typeOf env = function
  | MethodCall (target, (Identifier name), paramExps) ->
      typeOfMethodCall env target name paramExps
  | ArrayIndex (target, index) ->
      typeOfMethodCall env target "[]" [index]
  | FuncCall (expr, paramExps) ->
      let paramTypes = List.map (typeOf env) paramExps
      match typeOf env expr with
      | Function (name, paramTypes', returnType) ->
          //let paramTypes'' = List.map (fun (l:Lazy<Type>) -> l.Force()) paramTypes'
          //if paramTypes <> paramTypes''
          //  then printfn "Invalid parameters to function %s(). Expecting %A, got %A" name paramTypes'' paramTypes
          returnType.Force()
      | other -> failwithf "The target expression %A should be a function but is a %A" expr other
  | BinaryExpr (oper, expr1, expr2) ->
    let type1 = typeOf env expr1
    let type2 = typeOf env expr2
    typeOfOp (oper, type1, type2)
  | expr.SymbolExpr _ -> Symbol
  | Id (Identifier name) ->
      match Map.tryfind name env with
      | Some v -> v
      | _ -> failwithf "typeOf: Unknown variable or identifier: %s" name  
  | Integer v -> Int


      (*
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
  | BinaryExpr (oper, expr1, expr2) ->
    let value1 = eval env expr1
    let value2 = eval env expr2
    evalOp (oper, value1, value2)
  | Id (Identifier name) ->
      match Map.tryfind name env with
      | Some v -> v
      | _ -> failwithf "eval: Unknown variable or identifier: %s" name
  | expr.Integer v -> VInt v
  *)
  | other -> failwithf "Not implemented: %A" other


and typeOfMethodCall env target name paramExps =
  let targetType = typeOf env target
  match targetType with
  | Class (cname, methods) ->
      match Map.tryfind name methods with
      | Some (Function (fname, _, returnType)) -> returnType.Force()
      | _ -> failwithf "Method not found in class %s: %s" cname name
  | _ -> failwith "Target is not a class type"

and typeOfOp = function
  | Plus, Int, Int -> Int
  | GreaterThan, Int, Int -> Bool
 // | _, (Class ("dynValWindow", _) as c), _ -> c
 // | _, _, (Class ("dynValWindow", _) as c) -> c
  | x -> failwithf "typeOfOp: op not implemented %A" x


let initialEnv =
  let predicateType = Function ("", [lazy Event], lazy Bool)
  let projType = Function ("", [lazy Any], lazy Any)
 
  // Dynamic values
  let rec dynValWindowType = Class ("dynValWindow", Map.of_list ["last", lastType; "sum", sumType])
  and dynValType = Class ("dynVal", Map.of_list ["[]", createWindowType])
  and lastType = Function ("last", [lazy Symbol], lazy dynValType)
  and sumType = Function ("sum", [lazy Symbol], lazy dynValType)
  and createWindowType = Function ("[]", [lazy Type.Time], lazy dynValWindowType)
 
 
  let rec dictType = Class ("dictionary", Map.of_list ["where", whereType; "select", selectType])
  and whereType = Function ("where", [lazy predicateType], lazy dictType)
  and selectType = Function ("select", [lazy projType], lazy dictType)
  
  let rec windowType = Class ("window", Map.of_list ["last", lastType; "where", whereType
                                                     "groupby", groupbyType
                                                     "sum", sumType])
  and lastType = Function ("last", [lazy Symbol], lazy dynValType)
  and sumType = Function ("sum", [lazy Symbol], lazy dynValType)
  and whereType = Function ("where", [lazy predicateType], lazy windowType)
  and grpbySubType = Function ("", [lazy windowType], lazy Any)
  and groupbyType = Function ("groupby", [lazy grpbySubType], lazy dictType)
  
  let rec streamType = Class ("stream", Map.of_list ["last", lastType; "where", whereType
                                                     "groupby", groupbyType; "[]", createWindowType
                                                     "sum", sumType; "select", selectType])
  and lastType = Function ("last", [lazy Symbol], lazy dynValType)
  and sumType = Function ("sum", [lazy Symbol], lazy dynValType)
  and whereType = Function ("where", [lazy predicateType], lazy streamType)
  and selectType = Function ("select", [lazy predicateType], lazy streamType)
  and grpbySubType = Function ("", [lazy streamType], lazy Any)
  and groupbyType = Function ("groupby", [lazy grpbySubType], lazy dictType)
  and createWindowType = Function ("[]", [lazy Type.Time], lazy windowType)
  
  Map.of_list ["stream", Function ("stream", [lazy Symbol], lazy streamType)]
