#light

open Ast
open Types

type WindowType =
  | TimedWindow of int

type Type =
  | TyUnit
  | TyBool
  | TyInt
  | TyString
  | TySymbol
  | TyType of Map<string, Type>
  | TyRecord of Map<string, Type>
  | TyStream of string list
  | TyWindow of Type * WindowType
  | TyDict of Type
  
  override self.ToString() =
    match self with
    | TyUnit -> "()"
    | TyBool -> "bool"
    | TyInt -> "int"
    | TyString -> "string"
    | TySymbol -> "symbol"
    | TyType _ -> "type"
    | TyRecord _ -> "record"
    | TyStream _ -> "stream"
    | TyWindow _ -> "window"
    | TyDict _ -> "dict"


and TypeContext = Map<string, Type>

let rec types (env:TypeContext) = function
  | Def (Identifier name, expr) ->
      let typ = typeOf env expr
      env.Add(name, typ)
  | Expr expr -> typeOf env expr |> ignore
                 env
  | Entity (Identifier name, ((source, uniqueId), assocs, members)) ->
      match typeOf env source with
      | TyStream fields ->
          let members1 = List.fold_left (fun (acc:TypeContext) assoc -> 
                                           match assoc with
                                           | BelongsTo (Symbol entity) ->
                                               let fieldType = match env.[String.capitalize entity] with
                                                               | TyType fields -> TyRecord fields
                                                               | _ -> failwithf "The entity is not an entity :S"
                                               acc.Add(entity, fieldType))
                                        (Map.of_list [ for f in fields -> (f, TyInt)]) assocs
                                        
          let members2 = List.fold_left (fun (acc:TypeContext) (Member (Identifier self, Identifier name, expr)) ->
                                           let selfType = TyRecord acc
                                           Map.add name (typeOf (env.Add(self, selfType)) expr) acc)
                                        members1 members
          env.Add(name, TyType members2)
      | _ -> failwithf "The source of the entity '%s' is not a stream" name

and typeOf env = function
  | MethodCall (target, (Identifier name), paramExps) ->
      typeOfMethodCall env target name paramExps
  | ArrayIndex (target, index) ->
      typeOfMethodCall env target "[]" [index]
  | FuncCall (Id (Identifier "stream"), fields) ->
      let fields' = [ for f in fields ->
                        match f with
                        | SymbolExpr (Symbol name) -> name
                        | _ -> failwithf "Invalid arguments to 'stream': %A" f ]
      TyStream ("timestamp"::fields')
  | FuncCall (Id (Identifier "when"), [source; Lambda ([Identifier ev], handler)]) ->
      match typeOf env source with
      | TyStream _ -> ()
      | _ -> failwithf "The source of the when doesn't reduce to a stream"
      let evType = TyRecord (Map.of_list ["timestamp", TyInt; "value", TyInt])
      typeOf (env.Add(ev, evType)) handler |> ignore
      TyUnit
  | FuncCall (Id (Identifier "print"), [expr]) ->
      typeOf env expr |> ignore
      TyUnit
  | MemberAccess (target, Identifier name) ->
      let targetType = typeOf env target
      match targetType with
      | TyRecord fields -> match Map.tryfind name fields with
                           | Some t -> t
                           | None -> failwithf "The record doesn't have field '%s'" name
      | TyType fields -> match name with
                         | "all" -> TyDict (TyRecord fields)
                         | _ -> failwithf "The type %A does not have field '%s'" targetType name
      | _ -> failwithf "The target type %A doesn't have any fields." targetType
  | BinaryExpr (oper, expr1, expr2) ->
    let type1 = typeOf env expr1
    let type2 = typeOf env expr2
    typeOfOp (oper, type1, type2)
  | Record fields ->
      let fieldTypes = [ for (Symbol name, expr) in fields -> (name, typeOf env expr) ]
      TyRecord (Map.of_list fieldTypes)
  | RecordWith (source, newFields) ->
      match typeOf env source with
      | TyRecord fields -> TyRecord (List.fold_left (fun fields (Symbol name, expr) -> fields.Add(name, typeOf env expr)) fields newFields)
      | _ -> failwith "Source is not a record!"
  | Let (Identifier name, binder, body) -> typeOf (env.Add(name, typeOf env binder)) body
  | If (cond, thn, els) ->
      let tyCond = typeOf env cond
      let tyThn = typeOf env thn
      let tyEls = typeOf env els
      if tyCond <> TyBool then failwith "If: The condition doesn't return bool!"
      if tyThn <> tyEls then failwith "If: Then and else have different return types."
      tyThn
  | Seq (expr1, expr2) ->
      typeOf env expr1 |> ignore
      typeOf env expr2
  | Id (Identifier name) ->
      match Map.tryfind name env with
      | Some v -> v
      | _ -> failwithf "typeOf: Unknown variable or identifier: %s" name  
  | SymbolExpr _ -> TySymbol
  | Integer v -> TyInt
  | String s -> TyString
  | Bool b -> TyBool
  | other -> failwithf "typeOf: Not implemented: %A" other


and typeOfMethodCall env target name paramExps =
  let targetType = typeOf env target
  match targetType with
  | TyStream fields ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when List.mem field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              let evType = TyRecord (Map.of_list (("timestamp", TyInt)::[ for f in fields -> (f, TyInt) ]))
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              let evType = TyRecord (Map.of_list (("timestamp", TyInt)::[ for f in fields -> (f, TyInt) ]))
              match typeOf (env.Add(ev, evType)) expr with
              | TyRecord projFields -> TyStream ((Map.to_list >> (List.map fst)) projFields)
              | _ -> failwithf "The projector of the select doesn't return a record!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps        
      | "[]" ->
          match paramExps with
          | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Identifier g], expr)] when List.mem field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  // Event Windows
  | TyWindow (TyStream fields, TimedWindow _) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when List.mem field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              let evType = TyRecord (Map.of_list (("timestamp", TyInt)::[ for f in fields -> (f, TyInt) ]))
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Identifier g], expr)] when List.mem field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | TyWindow (TyInt, TimedWindow _) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [] -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | TyDict valueType ->
      match name with
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier g], expr)] -> if (typeOf (env.Add(g, valueType)) expr) <> TyBool
                                                 then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          let valueType' = match paramExps with
                           | [Lambda ([Identifier g], expr)] -> typeOf (env.Add(g, valueType)) expr
                           | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          TyDict valueType'
      | "[]" -> match paramExps with
                | [index] -> valueType
                | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | TyInt ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [] -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "[]" -> match paramExps with
                | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
                | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "updated" -> match paramExps with
                     | [] -> TyStream ["value"]
                     | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  | _ -> failwithf "The type %A does not have method %A!" targetType name
 

and typeOfOp = function
  | Plus, TyInt, TyInt -> TyInt
  | Minus, TyInt, TyInt -> TyInt
  | Times, TyInt, TyInt -> TyInt
  | GreaterThan, TyInt, TyInt -> TyBool
  | GreaterThanOrEqual, TyInt, TyInt -> TyBool
  | Equal, TyInt, TyInt -> TyBool
  | NotEqual, TyInt, TyInt -> TyBool
  | LessThanOrEqual, TyInt, TyInt -> TyBool
  | LessThan, TyInt, TyInt -> TyBool
  | Plus, _, TyString -> TyString
  | Plus, TyString, _ -> TyString
  | x -> failwithf "typeOfOp: op not implemented %A" x
