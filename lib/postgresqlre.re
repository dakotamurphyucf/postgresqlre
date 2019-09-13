open Lwt.Infix;

open Result;

type connection = Postgresql.connection;

// TODO implement Error interface Around this So that we can abstract away dealing with Postgresql errors
type error = Postgresql.error;

module type QUERYABLE = {
  type t;
  let one:
    (~query: string, ~params: array(string)=?, t) => Lwt.t(result(option(array(string)), error));
  let all:
    (~query: string, ~params: array(string)=?, t) => Lwt.t(result(array(array(string)), error));
  let command: (~query: string, ~params: array(string)=?, t) => Lwt.t(result(unit, error));
  let command_returning:
    (~query: string, ~params: array(string)=?, t) => Lwt.t(result(array(array(string)), error));
};

type t = connection;

let connect = (~conninfo) =>
  Lwt_preemptive.detach(
    () =>
      try (Ok((new Postgresql.connection)(~conninfo, ()))) {
      | Postgresql.Error(e) => Error(Postgresql.string_of_error(e))
      }
  );

let rec wait_for_result = (conn: connection) => {
  conn#consume_input;
  if (conn#is_busy) {
    Lwt_unix.yield() >>= (() => wait_for_result(conn));
  } else {
    switch conn#get_result {
    | None => Lwt.return(Ok(None))
    | Some(result) =>
      /* Free up the connection. */
      assert (conn#get_result == None);
      switch result#status {
        | Bad_response
        | Nonfatal_error
        | Fatal_error => {
          let code = Postgresql.Error_code.to_string(result#error_code)
          let errmsg = result#error
          Lwt.return(Error((code, errmsg)))
        }
        | _ => Lwt.return(Ok(Some(result)))
      };
    };
  };
};

let send_query_and_wait = (query, params, conn: connection) =>
  Lwt.catch(
    () => {
      conn#send_query(~params, query);
      wait_for_result(conn);
    },
    fun
    | e => Lwt.fail(e)
  );

let one = (~query, ~params=[||], conn: connection) =>
  Lwt_result.Infix.(
    send_query_and_wait(query, params, conn)
    >>= (
      fun
      | None => Lwt_result.return(None)
      | Some(result) => {
        switch result#status {
        | Tuples_ok => result#ntuples > 0 ? Some(result#get_tuple(0)) |> Lwt_result.return : Lwt_result.return(None)
        | _ => Lwt_result.fail(("EXPECTED_ROW_RETURN", "Query expected row to be returned"))
        };
      }
    )
  );

let all = (~query, ~params=[||], conn) =>
  Lwt_result.Infix.(
    send_query_and_wait(query, params, conn)
    >|= (
      fun
      | None => [||]
      | Some(result) => result#get_all
    )
  );

let command = (~query, ~params=[||], conn) =>
  Lwt_result.Infix.(send_query_and_wait(query, params, conn) >|= ((_) => ()));

/* command_returning has the same semantic as all.
   We're keeping them separate for clarity. */
let command_returning = all;

let finish = (conn) =>
  Lwt_preemptive.detach(
    (c: connection) =>
      try (Ok(c#finish)) {
      | Postgresql.Error(e) => Error(Postgresql.string_of_error(e))
      },
    conn
  );

module Pool = {
  type t = Lwt_pool.t(connection);
  let create = (~conninfo, ~size, ()) =>
    Lwt.Infix.(
      Lwt_pool.create(
        size,
        () =>
          connect(~conninfo, ())
          >>= (
            fun
            | Ok(conn) => Lwt.return(conn)
            | Error(_e) => failwith @@ "postgresqlre: Failed to connect. Conninfo=" ++ conninfo
          )
      )
    );
  let one = (~query, ~params=[||], pool) => Lwt_pool.use(pool, one(~query, ~params));
  let all = (~query, ~params=[||], pool) => Lwt_pool.use(pool, all(~query, ~params));
  let command = (~query, ~params=[||], pool) => Lwt_pool.use(pool, command(~query, ~params));
  let command_returning = all;
};

module Transaction = {
  type t = connection;
  let begin_ = (conn: connection) =>
    command(~query="BEGIN", conn)
    >>= (
      (res) =>
        switch res {
        | Ok () => Ok(conn) |> Lwt.return
        | Error(e) => Error(e) |> Lwt.return
        }
    );
  let commit = (trx: t) => command(~query="COMMIT", trx);
  let rollback = (trx: t) => command(~query="ROLLBACK", trx);
  let one = one;
  let all = all;
  let command = command;
  let command_returning = all;
  module Pool = {
    let begin_ = (pool) => Lwt_pool.use(pool, begin_);
  };
};