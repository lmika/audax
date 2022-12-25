package scriptmanager

import (
	"context"
	"github.com/cloudcmds/tamarin/arg"
	"github.com/cloudcmds/tamarin/object"
	"github.com/cloudcmds/tamarin/scope"
	"github.com/lmika/audax/internal/dynamo-browse/models"
)

type sessionModule struct {
	sessionService SessionService
}

func (um *sessionModule) query(ctx context.Context, args ...object.Object) object.Object {
	if err := arg.Require("session.query", 1, args); err != nil {
		return err
	}

	expr, _ := object.AsString(args[0])
	resp, err := um.sessionService.Query(ctx, expr)

	if err != nil {
		return object.NewErrorResult("%v", err)
	}
	return object.NewOkResult(&resultSetProxy{resultSet: resp})
}

func (um *sessionModule) resultSet(ctx context.Context, args ...object.Object) object.Object {
	if err := arg.Require("session.result_set", 0, args); err != nil {
		return err
	}

	rs := um.sessionService.ResultSet()
	if rs == nil {
		return object.Nil
	}
	return &resultSetProxy{resultSet: rs}
}

func (um *sessionModule) register(scp *scope.Scope) {
	modScope := scope.New(scope.Opts{})
	mod := &object.Module{Name: "session", Scope: modScope}

	modScope.AddBuiltins([]*object.Builtin{
		{Name: "query", Module: mod, Fn: um.query},
		{Name: "result_set", Module: mod, Fn: um.resultSet},
	})

	scp.Declare("session", mod, true)
}

type resultSetProxy struct {
	resultSet *models.ResultSet
}

func (r *resultSetProxy) Type() object.Type {
	return "ResultSet"
}

func (r *resultSetProxy) Inspect() string {
	return "ResultSet"
}

func (r *resultSetProxy) ToInterface() interface{} {
	return r.resultSet
}

func (r *resultSetProxy) Equals(other object.Object) object.Object {
	// TODO
	return object.False
}

func (r *resultSetProxy) GetAttr(name string) (object.Object, bool) {
	// TODO: this should implement the container interface
	switch name {
	case "length":
		return object.NewInt(int64(len(r.resultSet.Items()))), true
	}

	return nil, false
}
