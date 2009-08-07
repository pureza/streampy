type prog =
  | Expr of expr
  | Def of id * expr * option<listener list>
  | Function of id * param list * expr
  | DefVariant of id * (id * Type) list
  | StreamDef of id * Type

and expr =
  | Let of expr * Type option * expr * expr
  | LetListener of expr * Type option * expr * listener list * expr
  | BinaryExpr of op * expr * expr
  | MemberAccess of expr * id
  | MethodCall of expr * id * expr list
  | ArrayIndex of expr * expr
  | FuncCall of expr * expr
  | Lambda of param list * expr
  | If of expr * expr * expr
  | Match of expr * matchCase list
  | Seq of expr * expr
  | Record of (string * expr) list
  | Tuple of expr list
  | Integer of int
  | Float of single
  | Bool of bool
  | String of string
  | Id of id
  | SymbolExpr of symbol
  | Fail // Represents a failed pattern match.
  | Null

  member self.Name =
    match self with
    | Let (pattern, _, _, _) -> sprintf "let %O = ... in" pattern
    | LetListener (pattern, _, _, _, _) -> sprintf "let %O = ... in" pattern
    | BinaryExpr (op, _, _) -> sprintf "BinaryExpr(%A)" op
    | MethodCall (target, Identifier name, _) -> sprintf "%s.%s(...)" target.Name name
    | FuncCall (func, _) -> sprintf "%s(...)" func.Name
    | MemberAccess _ -> "MemberAccess"
    //| FixedAccess expr -> sprintf "Fixed(%s)" expr.Name
    | Lambda _ -> "Lambda"
    | If _ -> "if"
    | Match _ -> "match"
    | ArrayIndex _ -> "ArrayIndex"
    | Seq (expr1, expr2) -> sprintf "%s; %s" expr1.Name expr2.Name
    | Record _ -> "Record"
    | Tuple _ -> "tuple"
    //| RecordWith _ -> "RecordWith"
    | Integer i -> i.ToString()
    | Float f -> f.ToString()
    | Bool b -> b.ToString()
    | String s -> sprintf "\"%O\"" s
   // | Null -> "null"
    | SymbolExpr (Symbol sym) -> sprintf ":%s" sym
    | Id (Identifier id) -> id
    | Fail -> "<fail>"
    | Null -> "<null>"

and op =
  | GreaterThan
  | GreaterThanOrEqual
  | Equal
  | NotEqual
  | LessThanOrEqual
  | LessThan
  | And
  | Or
  | Plus
  | Minus
  | Times
  | Div
  | Mod
  | Is // value is variant-label

and id = Identifier of string

and param = Param of expr * Type option

and matchCase = MatchCase of expr * expr // pattern -> expr

and symbol = Symbol of string

and listener = Listener of id option * expr * expr option * expr

and Type =
  | TyUnit
  | TyBool
  | TyInt
  | TyFloat
  | TyString
  | TySymbol
  | TyArrow of Type * Type
  | TyGen of int    // int < 0 means that this is a user type annotation.
  | TyRecord of Map<string, Type>
  | TyTuple of Type list
  | TyVariant of id * (id * Type) list
  | TyPrimitive of PrimTyChecker
  | TyAlias of string
  | TyStream of Type
  | TyWindow of Type
  | TyDict of Type
  | TyUnknown of Type

  override self.ToString() =
    match self with
    | TyUnit -> "unit"
    | TyBool -> "bool"
    | TyInt -> "int"
    | TyFloat -> "float"
    | TyString -> "string"
    | TySymbol -> "symbol"
    | TyArrow (type1, type2) -> sprintf "%O -> %O" type1 type2
    | TyGen typ -> sprintf "'%d" typ
    | TyRecord _ -> "record"
    | TyTuple _ -> "tuple"
    | TyVariant (Identifier id, cases) -> sprintf "%s<%s>" id (List.fold (fun acc (Identifier name, _) -> acc + "|" + name) "" cases)
    | TyPrimitive _ -> "primitive function"
    | TyAlias typ -> sprintf "alias<%s>" typ
    | TyStream typ -> sprintf "stream<%O>" typ
    | TyWindow typ -> sprintf "win<%O>" typ
    | TyDict typ -> sprintf "dict<%O>" typ
    | TyUnknown t -> sprintf "unk<%O>" t

  member self.IsPrimitive () =
    match self with
    | TyPrimitive _ -> true
    | _ -> false

  member self.IsUnknown () =
    match self with
    | TyUnknown _ -> true
    | _ -> false


and PrimTyChecker = TypeContext -> expr -> Type * (Type * Type) list
and TypeContext = Map<string, Type * int>

let getType = fst



let freeVars expr =
  let rec freeVars' boundVars expr =
    match expr with
    | MethodCall (target, (Identifier name), paramExps) ->
        List.fold (fun acc paramExpr -> Set.union acc (freeVars' boundVars paramExpr))
                  (freeVars' boundVars target) paramExps
    | ArrayIndex (target, index) ->
        Set.union (freeVars' boundVars target) (freeVars' boundVars index)
    | FuncCall (fn, param) -> Set.union (freeVars' boundVars fn) (freeVars' boundVars param)
    | MemberAccess (target, (Identifier name)) -> freeVars' boundVars target
    //| FixedAccess expr -> freeVars' boundVars expr
    | Record fields ->
        List.fold (fun acc (_, fieldExpr) -> Set.union acc (freeVars' boundVars fieldExpr))
                  Set.empty fields
    //| RecordWith (record, fields) ->
    //    List.fold (fun acc (_, fieldExpr) -> Set.union acc (freeVars' boundVars fieldExpr))
    //              (freeVars' boundVars record) fields
    | Tuple elements ->
        List.fold (fun acc elt -> Set.union acc (freeVars' boundVars elt))
                  Set.empty elements
    | Lambda (args, body) ->
        let boundVars' = Set.union (List.fold (fun acc (Param (pattern, _)) ->
                                                 match pattern with
                                                 | Id (Identifier name) -> Set.add name acc
                                                 | _ -> failwithf "Can't happen because rewrite is supposed to eliminate pattern parameters.")
                                              Set.empty args)
                                   boundVars
        freeVars' boundVars' body
    | Let (Id (Identifier name), optType, binder, body) ->
        let freeBinder = freeVars' boundVars binder
        let boundVars' = Set.add name boundVars
        Set.union freeBinder (freeVars' boundVars' body)
    | Let _ -> failwithf "Can't happen because rewrite is supposed to eliminate pattern parameters."
    | If (cond, thn, els) -> Set.union (freeVars' boundVars cond) (Set.union (freeVars' boundVars thn) (freeVars' boundVars els))
 //   | Match (expr, cases) ->
 //       let freeExpr = freeVars' boundVars expr
 //       List.fold (fun acc (MatchCase (_, metaOpt, body)) ->
 //                    let freeCase =
 //                      match metaOpt with
 //                      | Some (Identifier meta) -> freeVars' (boundVars.Add(meta)) body
 //                      | None -> freeVars' boundVars body
 //                    Set.union acc freeCase)
 //                 freeExpr cases
    | BinaryExpr (oper, expr1, expr2) as expr -> Set.union (freeVars' boundVars expr1) (freeVars' boundVars expr2)
    | Seq (expr1, expr2) -> Set.union (freeVars' boundVars expr1) (freeVars' boundVars expr2)
    | Id (Identifier name) -> if Set.contains name boundVars then Set.empty else Set.singleton name
    | Integer _ | Float _ | String _ | Bool _ | SymbolExpr _ | Fail | Null -> Set.empty

  freeVars' Set.empty expr


let isRecursive name expr =
  match expr with
  | Lambda (args, body) -> Set.contains name (freeVars expr)
  | _ -> false


(* May a node be created to evaluate expr?
 * No, if node depends on some variable explicitly marked as Unknown. *)
let rec isContinuous (env:TypeContext) expr =
  match expr with
  | MethodCall (target, (Identifier name), paramExps) ->
      (isContinuous env target) && List.forall (isContinuous env) paramExps
  | ArrayIndex (target, index) ->
      (isContinuous env target) && (isContinuous env index)
  | FuncCall (fn, param) ->
      isContinuous env fn && isContinuous env param
  | MemberAccess (target, (Identifier name)) -> isContinuous env target
//  | FixedAccess expr -> isContinuous env expr
  | Record fields -> List.forall (isContinuous env) (List.map snd fields)
  | Tuple elts -> List.forall (isContinuous env) elts
//  | RecordWith (record, fields) -> isContinuous env record && List.forall (isContinuous env) (List.map snd fields)
  | Lambda (args, body) -> isContinuous env body
  | Let (_, optType, binder, body) -> isContinuous env binder && isContinuous env body
  | If (cond, thn, els) -> isContinuous env cond && isContinuous env thn && isContinuous env els
  | Match (expr, cases) -> isContinuous env expr && List.forall (fun (MatchCase (pattern, body)) -> isContinuous env pattern && isContinuous env body) cases
  | BinaryExpr (oper, expr1, expr2) as expr -> isContinuous env expr1 && isContinuous env expr2
  | Seq (expr1, expr2) -> isContinuous env expr1 && isContinuous env expr2
  | Id (Identifier name) -> if Map.contains name env && ((getType env.[name]).IsUnknown()) then false else true
  | Integer _ | Float _ | String _ | Bool _ | SymbolExpr _ | Fail | Null -> true


let isContinuousType typ =
  match typ with
  | TyStream _ -> false
  | TyWindow (TyStream _) -> false
  | TyDict _ -> false
  | _ -> true


(*
 * Replaces the given expression with another.
 *)
let rec visit expr visitor =
  match expr with
  | FuncCall (fn, param) -> FuncCall (visitor fn, visitor param)
  | MethodCall (target, name, paramExps) -> MethodCall (visitor target, name, List.map visitor paramExps)
  | ArrayIndex (target, index) -> ArrayIndex (visitor target, visitor index)
  | MemberAccess (target, name) -> MemberAccess (visitor target, name)
  | Lambda (ids, expr) -> Lambda (ids, visitor expr)
  | BinaryExpr (op, left, right) -> BinaryExpr (op, visitor left, visitor right)
  | Let (pattern, optType, binder, body) -> Let (visitor pattern, optType, visitor binder, visitor body)
  | LetListener (pattern, optType, binder, listeners, body) ->
      let listeners' = List.map (fun (Listener (evOpt, stream, guardOpt, body)) ->
                                       Listener (evOpt, visitor stream, Option.bind (visitor >> Some) guardOpt, visitor body))
                                listeners
      LetListener (visitor pattern, optType, visitor binder, listeners', visitor body)
  | If (cond, thn, els) -> If (visitor cond, visitor thn, visitor els)
  | Match (expr, cases) -> Match (visitor expr, List.map (fun (MatchCase (pattern, body)) -> MatchCase (pattern, visitor body)) cases)
  | Seq (expr1, expr2) -> Seq (visitor expr1, visitor expr2)
  | Record fields -> Record (List.map (fun (n, e) -> (n, visitor e)) fields)
  | Tuple fields -> Tuple (List.map visitor fields)
  | Id _ | Integer _ | Float _ | String _ | Bool _ | SymbolExpr _ | Fail | Null -> expr  