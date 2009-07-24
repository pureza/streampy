#light

open Ast
open Util
open Types
open Extensions



type TypeContext = Map<string, Type>
and RemainingMap = Map<string, TypeContext -> TypeContext>

let matchingEntity fields types =
  Map.tryPick (fun k v -> match v with
                          | TyType (_, fields', _, _) when fields = fields' -> Some k
                          | _ -> None)
              types

let rec resolveAlias (env:TypeContext) typ =
  match typ with
  | TyAlias typ' -> env.[typ']
  | TyRecord fields -> TyRecord (Map.map (fun k v -> resolveAlias env v) fields)
  | TyArrow (inp, out) -> TyArrow (resolveAlias env inp, resolveAlias env out)
  | _ -> typ

let rec types (env:TypeContext) = function
  | DefVariant (Identifier name, variants) ->
      let variantType = TyVariant (Identifier name, variants)
      let env' = env.Add(name, variantType)
      let env'' = List.fold (fun (env:TypeContext) (Identifier variant, meta) ->
                               env.Add(variant, TyArrow (resolveAlias env meta, variantType)))
                            env' variants
      env''
  | Def (Identifier name, expr, listenersOpt) ->
      let typ = typeOf env expr
      match listenersOpt with
      | Some listeners -> checkListeners env name typ listeners
      | _ -> ()
      env.Add(name, typ)
  | Expr expr -> typeOf env expr |> ignore
                 env
  | Function (Identifier name, parameters, retType, body) ->
      let retType' = resolveAlias env retType
      let env', fnType =
        List.foldBack (fun (Param (Identifier param, typ)) (env:TypeContext, fnType)  ->
                         match typ with
                         | Some t -> let t' = resolveAlias env t
                                     env.Add(param, t'), TyArrow (t', fnType)
                         | _ -> failwithf "You must annotate all function arguments with their type!")
                      parameters (env, retType')
      // Add itself
      let fnType' = if parameters.IsEmpty then TyArrow (TyUnit, fnType) else fnType
      let env'' = env'.Add(name, fnType')
      if typeOf env'' body = retType'
        then env.Add(name, fnType')
        else failwithf "The function body doesn't return %A" retType
  | StreamDef (Identifier name, fields) ->
      let fields' = List.map (fun (Identifier f, t) -> (f, t)) fields |> Map.of_list |> Map.add "timestamp" TyInt
      env.Add(name, TyStream (TyRecord fields'))
  | Entity (Identifier ename, ((source, Symbol uniqueId), assocs, members)) ->
      match typeOf env source with
      | TyStream (TyRecord fields) as streamType ->
          let autoGenFields = fields.Add("events", streamType)
          let members1, belongsTo =
            List.fold (fun (acc:TypeContext, belongsTo) assoc -> 
                         match assoc with
                         | BelongsTo (Symbol entity) ->
                             let entityName = String.capitalize entity
                             acc.Add(entity, TyRef (TyAlias entityName)), entityName::belongsTo
                         | HasMany (Symbol entity) ->
                             let entityName = String.capitalize entity |> String.singular
                             acc.Add(entity, TyDict (TyRef (TyAlias entityName))), belongsTo)
                      (autoGenFields, []) assocs

          let members2 = List.fold (fun (acc:TypeContext) (Member (Identifier self, Identifier name, expr, listenersOpt)) ->
                                      let selfType = TyRecord acc
                                      let env' = env.Add(self, selfType)
                                      let fieldType = typeOf env' expr
                                      
                                      match listenersOpt with
                                      | Some listeners ->
                                          let selfType' = TyType (ename, (acc.Add(name, fieldType)), uniqueId, [])
                                          let env' = env.Add(self, selfType')
                                          checkListeners env' name fieldType listeners
                                      | None -> ()
                                      Map.add name fieldType acc)
                                   members1 members
                                   
          let entityType = TyType (ename, members2, uniqueId, belongsTo)
          env.Add(ename, entityType).Add(sprintf "$%s_all" ename, TyDict entityType)
      | _ -> failwithf "The source of the entity '%s' is not a stream" ename


and checkListeners env def defType listeners =
  for listener in listeners do
    match listener with
    | Listener (evOpt, stream, guardOpt, body) ->
        let env' = match evOpt with
                   | Some (Identifier ev) ->
                       let evType = match typeOf env stream with
                                    | TyStream evType -> evType
                                    | _ -> failwithf "Not a stream!"
                       env.Add(ev, evType)
                   | _ -> env
        
        let env'' = env'.Add(def, defType)
        match guardOpt with
        | None -> ()
        | Some expr when typeOf env'' expr = TyBool -> ()
        | _ -> failwithf "The guard doesn't return bool!"
        if typeOf env'' body <> defType then failwithf "The body of the listener doesn't return %A" defType


and typeOf env expr =
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      typeOfMethodCall env target name paramExps
  | ArrayIndex (target, index) ->
      typeOfMethodCall env target "[]" [index]
  | FuncCall _ -> typeOfFuncCall env expr
  | MemberAccess (_, Identifier "null?") -> TyBool
  | MemberAccess (target, Identifier name) ->
      let targetType = match typeOf env target with
                       | TyRef t -> t
                       | other -> other

      match targetType with
      | TyUnknown (TyRecord fields) | TyRecord fields ->
          match Map.tryFind name fields with
          | Some t -> t
          | None -> failwithf "The record doesn't have field '%s'" name
      | TyAlias t | TyType (t, _, _, _) ->
          let fields = match resolveAlias env targetType with
                       | TyType (_, fields, _, _) -> fields
                       | _ -> failwithf "The entity is not a TyType?!"
          if name = "all"
            then TyDict targetType
            else match Map.tryFind name fields with
                 | Some t -> t
                 | None -> failwithf "The entity doesn't have field '%s'" name
      | TyFixed (TyRef t, fixedExpr) ->
          let fields = match resolveAlias env t with
                       | TyType (_, fields, _, _) -> fields
                       | _ -> failwithf "The entity is not a TyType?!"
          match Map.tryFind name fields with
          | Some t -> TyFixed (t, MemberAccess (fixedExpr, Identifier name))
          | None -> failwithf "The entity doesn't have field '%s'" name
      | TyInt ->
          match name with
          | "sec" | "min" -> TyInt
          | _ -> failwithf "The type %A doesn't have field %s" targetType name 
      | _ -> failwithf "The target type %A doesn't have any fields, including '%s'" targetType name
  | FixedAccess expr -> TyFixed ((typeOf env expr), expr)
  | BinaryExpr (oper, expr1, expr2) ->
    let type1 = typeOf env expr1
    let type2 = typeOf env expr2
    typeOfOp (oper, type1, type2)
  | Record fields ->
      let fields = Map.of_list [ for (name, expr) in fields -> (name, typeOf env expr) ]
      match matchingEntity fields env with
      | Some entity -> env.[entity]
      | _ -> TyRecord fields
  | RecordWith (source, newFields) ->
      match typeOf env source with
      | TyRecord fields -> TyRecord (List.fold (fun fields (name, expr) -> fields.Add(name, typeOf env expr)) fields newFields)
      | _ -> failwith "Source is not a record!"
  | Let (Identifier name, optType, binder, body) ->
      match optType with
      | None -> typeOf (env.Add(name, typeOf env binder)) body
      | Some typ -> typeOf (env.Add(name, typ)) body
  | Lambda (args, expr) ->
      let env', argTypes =
        List.fold (fun (env:TypeContext, argTypes) (Param (Identifier id, typ)) ->
                     match typ with
                     | Some typ' -> let typ'' = resolveAlias env typ'
                                    env.Add(id, typ''), argTypes @ [typ'']
                     | None -> failwithf "I have no idea what's the type of argument %A" id)
                  (env, []) args
      let funType = List.foldBack (fun arg acc -> TyArrow (arg, acc)) (if args.IsEmpty then [TyUnit] else argTypes) (typeOf env' expr)
      funType
  | If (cond, thn, els) ->
      let tyCond = typeOf env cond
      let tyThn = typeOf env thn
      let tyEls = typeOf env els
      if tyCond <> TyBool then failwith "If: The condition doesn't return bool!"
      if tyThn <> tyEls then failwith "If: Then and else have different return types."
      tyThn
  | Match (expr, cases) ->
      let typeOfCase (MatchCase (Identifier label, meta, body)) =
        match meta with
        | Some (Identifier meta') ->
            let metaType = metaTypeForLabel env label
            typeOf (env.Add(meta', metaType)) body
        | None -> typeOf env body

      let exprType = typeOf env expr
      let resultType = typeOfCase cases.[0]
      if List.forall (fun case -> typeOfCase case = resultType) cases.Tail
        then resultType
        else failwithf "Not all cases return the same type."
  | Seq (expr1, expr2) ->
      typeOf env expr1 |> ignore
      typeOf env expr2
  | Id (Identifier name) ->
      match Map.tryFind name env with
      | Some t -> match t with
                  | TyAlias t' -> typeOf env (Id (Identifier t'))
                  | _ -> t
      | _ -> raise (UnknownId name)
  | SymbolExpr _ -> TySymbol
  | Integer v -> TyInt
  | Float f -> TyFloat
  | String s -> TyString
  | Bool b -> TyBool
  | Null -> TyNull
  | other -> failwithf "typeOf: Not implemented: %A" other


and typeOfMethodCall env target name paramExps =
  let targetType = match typeOf env target with
                   | TyFixed (t, _) -> t
                   | other -> other

  // Generic methods first
  match name with
  | "changes" | "updates" ->
    match paramExps with
    | [] -> TyStream (TyRecord (Map.of_list ["value", targetType]))
    | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "count" -> TyInt
  | "last" | "sum" | "max" | "min" | "avg" | "prev" ->
      match targetType, paramExps with
      | TyRecord fields, [SymbolExpr (Symbol field)] when Map.contains field fields -> fields.[field]
      | TyStream (TyRecord fields), [SymbolExpr (Symbol field)] when Map.contains field fields -> fields.[field]
      | TyWindow (typ, _), _ ->
          match typ, paramExps with
          | TyRef typ', [SymbolExpr (Symbol field)] ->
              match resolveAlias env typ' with
              | TyType (_, fields, _, _) when Map.contains field fields -> fields.[field]
              | _ -> failwithf "The alias does not refer to a TyType"
          | (TyType (_, fields, _, _) | TyRecord fields | TyStream (TyRecord fields)), [SymbolExpr (Symbol field)]
              when Map.contains field fields -> fields.[field]
          | _, [] -> TyInt
          | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps         
      | TyInt, [] -> TyInt
      | TyFloat, [] -> TyFloat
      | TyBool, [] -> TyBool
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps                 
  | "any?" | "all?" ->                   
      match targetType, paramExps with
      | TyDict _, _ -> failwithf "The type %A does not have method %A!" targetType name
      | (TyBool | TyWindow (TyBool, _)), [] -> TyBool
      | (TyStream v | TyWindow (TyStream v, _) | TyWindow (v, _)), [Lambda ([Param (Identifier arg, _)], expr)] ->
          if (typeOf (env.Add(arg, v)) expr) <> TyBool
            then failwithf "The predicate of %s doesn't return a boolean!" name
          TyBool
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
  | "where" ->
      match targetType, paramExps with
      | (TyStream (TyRecord _ as argType) | TyWindow (TyStream (TyRecord _ as argType), _) | TyDict argType),
          [Lambda ([Param (Identifier arg, _)], expr)] -> 
          if (typeOf (env.Add(arg, argType)) expr) <> TyBool
            then failwithf "The predicate of the where doesn't return a boolean!"
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps      
      targetType
  | "groupby" ->
      match targetType, paramExps with
      | (TyStream (TyRecord fields as evType) | TyWindow (TyStream (TyRecord fields as evType), _)),
        [SymbolExpr (Symbol field); Lambda ([Param (Identifier g, _)], expr)] when Map.contains field fields ->
                                                                                TyDict (typeOf (env.Add(g, targetType)) expr)
      | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps

  // Specific methods below               
  | _ -> match targetType with
          | TyStream (TyRecord fields as evType) ->
              match name with
              | "select" -> 
                  match paramExps with
                  | [Lambda ([Param (Identifier ev, _)], expr)] -> 
                      match typeOf (env.Add(ev, evType)) expr with
                      | TyRecord projFields -> TyStream (TyRecord (projFields.Add("timestamp", TyInt)))
                      | _ -> failwithf "The projector of the select doesn't return a record!"
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "[]" ->
                  match paramExps with
                  | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyDict valueType ->
              match name with
              | "select" -> 
                  let valueType' = match paramExps with
                                   | [Lambda ([Param (Identifier g, _)], expr)] -> typeOf (env.Add(g, valueType)) expr
                                   | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
                  TyDict valueType'
              | "[]" -> match paramExps with
                        | [index] -> valueType
                        | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "values" when paramExps = [] -> TyWindow (valueType, Unbounded)             
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyInt | TyFloat ->
              match name with          
              | "[]" -> match paramExps with
                        | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
                        | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyBool ->
              match name with          
              | "[]" -> match paramExps with
                        | [Time (Integer length, unit)] -> TyWindow (targetType, TimedWindow (toSeconds length unit))
                        | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "howLong" -> TyInt
              | _ -> failwithf "The type %A does not have method %A!" targetType name              
          | TyRecord _ -> typeOf env (FuncCall (MemberAccess (target, Identifier name), paramExps))
          | TyWindow (TyStream evType, SortedWindow) ->
              match name with
              | "[]" -> match paramExps with
                        | [expr] when typeOf env expr = TyInt -> evType
                        | _ -> failwithf "Invalid index in []"
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyWindow (TyStream evType, windowType) ->
              match name with
              | "select" ->
                  match paramExps with
                  | [Lambda ([Param (Identifier ev, _)], expr)] -> 
                      match typeOf (env.Add(ev, evType)) expr with
                      | TyRecord projFields -> TyWindow (TyStream (TyRecord (projFields.Add("timestamp", TyInt))), windowType)
                      | _ -> failwithf "The projector of the select doesn't return a record!"
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "sortBy" ->
                  match paramExps with
                  | [SymbolExpr (Symbol field)] ->
                      match evType with
                      | TyRecord fields when Map.contains field fields -> TyWindow (TyStream evType, SortedWindow)
                      | _ -> failwithf "The type %A does not have field %A" evType field
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "[]" -> match paramExps with
                        | [expr] when typeOf env expr = TyInt -> evType
                        | _ -> failwithf "Invalid index in []"
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | TyWindow (valueType, windowType) ->
              match name with
              | "sort" ->
                  match paramExps with
                  | [] -> targetType
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "sortBy" ->
                  match paramExps with
                  | [SymbolExpr (Symbol field)] ->
                      match valueType with
                      | TyRecord fields | TyType (_, fields, _, _) when Map.contains field fields -> targetType
                      | _ -> failwithf "The type %A does not have field %A" valueType field
                  | _ -> failwithf "Invalid parameters to method '%s': %A" name paramExps
              | "[]" -> match paramExps with
                        | [expr] when typeOf env expr = TyInt -> valueType
                        | _ -> failwithf "Invalid index in []"
              | _ -> failwithf "The type %A does not have method %A!" targetType name
          | _ -> failwithf "The type %A does not have method %A!" targetType name



and typeOfFuncCall env expr =
  match expr with
  | FuncCall (Id (Identifier "when"), [source; Lambda ([Param (Identifier ev, _)], handler)]) ->
      let evType = match typeOf env source with
                   | TyStream evType -> evType
                   | _ -> failwithf "The source of the when doesn't reduce to a stream"
      typeOf (env.Add(ev, evType)) handler
  | FuncCall (Id (Identifier "print"), [expr]) ->
      typeOf env expr |> ignore
      TyUnit
  | FuncCall (Id (Identifier "$ref"), [expr]) ->
      match typeOf env expr with
      | TyType (name, _, _, _) -> TyRef (TyAlias name)
      | other -> TyRef other
  | FuncCall (Id (Identifier "listenN"), param's) ->
      match param's with
      | initial::(l1::ls) ->
          let initialType = typeOf env initial
          let listenerType = typeOf env l1
          let allSame = List.forall (fun listener -> listenerType = typeOf env listener) ls
          if allSame
            then if listenerType = TyArrow (initialType, initialType)
                   then initialType
                   else failwithf "listenN: The type of the listener is not compatible with the type of the initial value - %A vs %A" initialType listenerType
            else failwithf "Not all the listeners have the same type."
      | _ -> failwithf "Invalid parameters to listenN."
  | FuncCall (Id (Identifier "now"), []) -> TyInt
  | FuncCall (Id (Identifier "merge"), [stream1; stream2; SymbolExpr (Symbol field)]) ->
      match typeOf env stream1, typeOf env stream2 with
      | TyStream (TyRecord fields1), TyStream (TyRecord fields2) ->
          if Map.contains field fields1 && Map.contains field fields2
            then TyStream (TyRecord (Map.merge (fun a b -> a) fields1 fields2))
            else failwithf "Both streams must contain field %A" field
      | _ -> failwithf "merge() can only be used on streams."
  | FuncCall (f, param's) ->
      let rec getReturnType argCount funType =
        match argCount, funType with
        | 0, _ -> funType
        | n, TyArrow (type1, type2) when n > 0 -> getReturnType (n - 1) type2
        | _ -> failwithf "%A must be of type TyArrow!" funType
      
      let rec arrow2list = function
        | TyArrow (type1, type2) -> type1::(arrow2list type2)
        | other -> [other]
        
      let funType = typeOf env f
      let argTypes = arrow2list funType |> Seq.take param's.Length |> Seq.to_list      
      let matching = List.forall2 (fun arg paramType -> let argType = typeOf env arg
                                                        argType = paramType || argType.IsFixed ())
                                  param's argTypes
      if matching
        then getReturnType (if param's.Length = 0 then 1 else param's.Length) funType // The if handles function calls with unit as the only argument.
        else failwithf "Parameters don't match argument types in function call"
  | _ -> failwithf "Not a FuncCall? %A" expr
         

and typeOfOp = function
  | Plus, _, TyString -> TyString
  | Plus, TyString, _ -> TyString
  | (Plus | Minus | Times | Div | Mod), TyInt, TyInt -> TyInt
  | (Plus | Minus | Times | Div | Mod), TyNull, _ 
  | (Plus | Minus | Times | Div | Mod), _, TyNull -> TyNull
  | (Plus | Minus | Times | Div), TyFloat, _
  | (Plus | Minus | Times | Div), _, TyFloat -> TyFloat
  | GreaterThan, (TyInt | TyFloat), (TyInt | TyFloat) -> TyBool
  | GreaterThanOrEqual, (TyInt | TyFloat), (TyInt | TyFloat) -> TyBool
  | Equal, a, b when a = b || a = TyNull || b = TyNull -> TyBool
  | NotEqual, a, b when a = b || a = TyNull || b = TyNull -> TyBool
  | (Equal | NotEqual), TyInt, TyFloat | (Equal | NotEqual), TyFloat, TyInt -> TyBool
  | LessThanOrEqual, (TyInt | TyFloat), (TyInt | TyFloat) -> TyBool
  | LessThan, (TyInt | TyFloat), (TyInt | TyFloat) -> TyBool
  | And, TyBool, TyBool -> TyBool
  | Or, TyBool, TyBool -> TyBool
  | x -> failwithf "typeOfOp: op not implemented %A" x


and metaTypeForLabel (env:TypeContext) label =
  match env.[label] with
  | TyArrow (metaType, _) -> metaType
  | _ -> failwithf "The given label '%s' is not a variant label." label



(* May a node be created to evaluate expr? 
 * No, if node depends on some variable explicitly marked as Unknown. *)  
let rec isContinuous (env:TypeContext) expr = 
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      (isContinuous env target) && List.forall (isContinuous env) paramExps
  | ArrayIndex (target, index) ->
      (isContinuous env target) && (isContinuous env index)
  | FuncCall (fn, paramExps) ->
      (isContinuous env fn) && List.forall (isContinuous env) paramExps
  | MemberAccess (target, (Identifier name)) -> isContinuous env target
  | FixedAccess expr -> isContinuous env expr
  | Record fields -> List.forall (isContinuous env) (List.map snd fields)
  | RecordWith (record, fields) -> isContinuous env record && List.forall (isContinuous env) (List.map snd fields)
  | Lambda (args, body) -> isContinuous env body
  | Let (Identifier name, optType, binder, body) -> isContinuous env binder && isContinuous env body
  | If (cond, thn, els) -> isContinuous env cond && isContinuous env thn && isContinuous env els
  | Match (expr, cases) -> isContinuous env expr && List.forall (fun (MatchCase (Identifier label, meta, body)) -> isContinuous env body) cases
  | BinaryExpr (oper, expr1, expr2) as expr -> isContinuous env expr1 && isContinuous env expr2
  | Seq (expr1, expr2) -> isContinuous env expr1 && isContinuous env expr2
  | Id (Identifier name) -> if Map.contains name env && (env.[name].IsUnknown()) then false else true
  | Time _ | Integer _ | Float _ | String _ | Null | SymbolExpr _ | Bool _ -> true


let isContinuousType typ =
  match typ with
  | TyStream _ -> false
  | TyWindow (TyStream _, _) -> false
  | TyDict _ -> false
  | _ -> true  