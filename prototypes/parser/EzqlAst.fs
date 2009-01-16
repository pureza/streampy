#light

type prog = Prog of clss list

and clss  = Class of string * mthd list

and mthd  = Method of string * id list * expr

and expr =
  | BooleanExpr of op * expr * expr
  | MethodCall of expr * id * expr list
  | Call of expr * expr list
  | New of id * expr list
  | Binding of id * expr * expr
  | Lambda of id list * expr
  | IfThenElse of expr * expr * expr option
  | Index of expr * expr
  | Tuple of expr list
  | Not of expr
  | Integer of int
  | Symbol of string
  | Id of id
  | Assign of string * expr

and op =
  | OpGt
  | OpGte
  | OpEq
  | OpDiff
  | OpLte
  | OpLt
  | OpAnd
  | OpOr

and id = Identifier of string

