﻿#light

open Ast
open Util
open Types
open Extensions

type WindowType =
  | TimedWindow of int

type Type =
  | TyUnit
  | TyBool
  | TyInt
  | TyString
  | TySymbol
  | TyType of Map<string, Type> * string (* string is the field that gives the unique id *)
  | TyEntity of string
  | TyRecord of Map<string, Type> * string (* string is an alias *)
  | TyStream of Type
  | TyWindow of Type * WindowType
  | TyDict of Type
  | TyRef of Type
  
  override self.ToString() =
    match self with
    | TyUnit -> "()"
    | TyBool -> "bool"
    | TyInt -> "int"
    | TyString -> "string"
    | TySymbol -> "symbol"
    | TyType _ -> "type"
    | TyEntity typ -> sprintf "instanceOf %O" typ
    | TyRecord _ -> "record"
    | TyStream _ -> "stream"
    | TyWindow _ -> "window"
    | TyDict _ -> "dict"
    | TyRef t -> sprintf "ref<%O>" t


and TypeContext = Map<string, Type>
and RemainingMap = Map<string, TypeContext -> TypeContext>

let matchingEntity fields types =
  Map.tryPick (fun k v -> match v with
                          | TyType (fields', _) when fields = fields' -> Some k
                          | _ -> None)
              types

let rec types (env:TypeContext) = function
  | Def (Identifier name, expr) ->
      let typ = typeOf env expr
      env.Add(name, typ)
  | Expr expr -> typeOf env expr |> ignore
                 env
  | Entity (Identifier name, ((source, Symbol uniqueId), assocs, members)) ->
      match typeOf env source with
      | TyStream (TyRecord (fields, alias)) ->
          let members1 =
            List.fold (fun (acc:TypeContext) assoc -> 
                         match assoc with
                         | BelongsTo (Symbol entity) ->
                             let entityName = String.capitalize entity
                             acc.Add(entity, TyRef (TyEntity entityName))
                         | HasMany (Symbol entity) ->
                             let entityName = String.capitalize entity |> String.singular
                             acc.Add(entity, TyDict (TyRef (TyEntity entityName))))
                      (fields) assocs
                                  
          let members2 = List.fold (fun (acc:TypeContext) (Member (Identifier self, Identifier name, expr)) ->
                                      let selfType = TyRecord (acc, "")
                                      Map.add name (typeOf (env.Add(self, selfType)) expr) acc)
                                   members1 members
                                   
          let entityType = TyType (members2, uniqueId)
          env.Add(name, entityType).Add(sprintf "$%s_all" name, TyDict (TyEntity name))
      | _ -> failwithf "The source of the entity '%s' is not a stream" name

and typeOf env expr =
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      typeOfMethodCall env target name paramExps
  | ArrayIndex (target, index) ->
      typeOfMethodCall env target "[]" [index]
  | FuncCall (Id (Identifier "stream"), fields) ->
      let fields' = [ for f in fields ->
                        match f with
                        | SymbolExpr (Symbol name) -> (name, TyInt)
                        | _ -> failwithf "Invalid arguments to 'stream': %A" f ]
      TyStream (TyRecord (Map.of_list (("timestamp", TyInt)::fields'), ""))
  | FuncCall (Id (Identifier "when"), [source; Lambda ([Identifier ev], handler)]) ->
      match typeOf env source with
      | TyStream _ -> ()
      | _ -> failwithf "The source of the when doesn't reduce to a stream"
      let evType = TyRecord (Map.of_list ["timestamp", TyInt; "value", TyInt], "")
      typeOf (env.Add(ev, evType)) handler |> ignore
      TyUnit
  | FuncCall (Id (Identifier "print"), [expr]) ->
      typeOf env expr |> ignore
      TyUnit
  | FuncCall (Id (Identifier "$ref"), [expr]) -> TyRef (typeOf env expr)
  | MemberAccess (target, Identifier name) ->
      let targetType = match typeOf env target with
                       | TyRef t -> t
                       | other -> other
                       
      match targetType with
      | TyRecord (fields, _) ->
          match Map.tryFind name fields with
          | Some t -> t
          | None -> failwithf "The record doesn't have field '%s'" name
      | TyEntity t ->
          let (TyType (fields, _)) = env.[t]
          match Map.tryFind name fields with
          | Some t -> t
          | None -> failwithf "The entity doesn't have field '%s'" name
      | TyType (fields, _) -> match name with
                              | "all" -> TyDict (TyRecord (fields, ""))
                              | _ -> failwithf "The type %A does not have field '%s'" targetType name
      | _ -> failwithf "The target type %A doesn't have any fields." targetType
  | BinaryExpr (oper, expr1, expr2) ->
    let type1 = typeOf env expr1
    let type2 = typeOf env expr2
    typeOfOp (oper, type1, type2)
  | Record fields ->
      let fields = Map.of_list [ for (Symbol name, expr) in fields -> (name, typeOf env expr) ]
      match matchingEntity fields env with
      | Some entity -> TyEntity entity
      | _ -> TyRecord (fields, "")
  | RecordWith (source, newFields) ->
      match typeOf env source with
      | TyRecord (fields, alias) -> TyRecord (List.fold (fun fields (Symbol name, expr) -> fields.Add(name, typeOf env expr)) fields newFields, "")
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
      match Map.tryFind name env with
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
  | TyStream (TyRecord (fields, alias) as evType) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when Map.contains field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "select" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              match typeOf (env.Add(ev, evType)) expr with
              | TyRecord (projFields, alias) as outEvType -> TyStream outEvType
              | _ -> failwithf "The projector of the select doesn't return a record!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps        
      | "[]" ->
          match paramExps with
          | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Identifier g], expr)] when Map.contains field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | _ -> failwithf "The type %A does not have method %A!" targetType name
  // Event Windows
  | TyWindow (TyStream (TyRecord (fields, alias) as evType), TimedWindow _) ->
      match name with
      | "last" | "sum" | "count" ->
          match paramExps with
          | [SymbolExpr (Symbol field)] when Map.contains field fields -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
      | "where" -> 
          match paramExps with
          | [Lambda ([Identifier ev], expr)] -> 
              if (typeOf (env.Add(ev, evType)) expr) <> TyBool
                then failwithf "The predicate of the where doesn't return a boolean!"
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
          targetType
      | "groupby" ->
          match paramExps with
          | [SymbolExpr (Symbol field); Lambda ([Identifier g], expr)] when Map.contains field fields -> TyDict (typeOf (env.Add(g, targetType)) expr)
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
                     | [] -> TyStream (TyRecord (Map.of_list ["value", TyInt], ""))
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
