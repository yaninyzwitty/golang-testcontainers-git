package repository

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
)

type User struct {
	Id        string
	Name      string
	AliasName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ListUsersResult struct {
	Users     []User
	PageToken []byte
}

var (
	ErrUserNotFound  = errors.New("user not found")
	ErrInvalidUUID   = errors.New("invalid UUID format")
	ErrInsertFailed  = errors.New("failed to insert user")
	ErrQueryFailed   = errors.New("failed to query user")
	ErrListingFailed = errors.New("failed to list users")
)

type UserRepository struct {
	Db *gocql.Session
}

func NewUserRepository(db *gocql.Session) *UserRepository {
	return &UserRepository{Db: db}
}

// --- DB INSERT ---
func (r *UserRepository) CreateUser(ctx context.Context, user User, userPassword string) error {
	now := time.Now()

	if err := r.Db.Query(
		`INSERT INTO init_sh_keyspace.users (id, name, alias_name, created_at, updated_at, email, password) 
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		user.Id, user.Name, user.AliasName, now, now, user.Email, userPassword,
	).WithContext(ctx).Exec(); err != nil {
		return ErrInsertFailed
	}

	return nil
}

// --- DB SELECT ---
func (r *UserRepository) GetUser(ctx context.Context, id string) (*User, error) {
	userID, err := gocql.ParseUUID(id)
	if err != nil {
		return nil, ErrInvalidUUID
	}

	var (
		name      string
		aliasName string
		email     string
		createdAt time.Time
		updatedAt time.Time
	)

	if err := r.Db.Query(
		`SELECT name, alias_name, created_at, updated_at, email 
		 FROM init_sh_keyspace.users WHERE id = ?`,
		userID,
	).WithContext(ctx).Consistency(gocql.One).Scan(&name, &aliasName, &createdAt, &updatedAt, &email); err != nil {
		if err == gocql.ErrNotFound {
			return nil, ErrUserNotFound
		}
		return nil, ErrQueryFailed
	}

	return &User{
		Id:        id,
		Name:      name,
		AliasName: aliasName,
		Email:     email,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

// --- DB LIST ---
func (r *UserRepository) ListUsers(ctx context.Context, pageLimit int32, pageToken []byte) (*ListUsersResult, error) {
	pageSize := int(pageLimit)
	if pageSize <= 0 {
		pageSize = 10
	}

	q := r.Db.Query(
		`SELECT id, name, alias_name, created_at, updated_at, email FROM init_sh_keyspace.users`,
	).PageSize(pageSize).WithContext(ctx)

	if len(pageToken) > 0 {
		q = q.PageState(pageToken)
	}

	iter := q.Iter()

	var users []User
	var (
		id        gocql.UUID
		name      string
		aliasName string
		createdAt time.Time
		updatedAt time.Time
		email     string
	)

	for iter.Scan(&id, &name, &aliasName, &createdAt, &updatedAt, &email) {
		users = append(users, User{
			Id:        id.String(),
			Name:      name,
			AliasName: aliasName,
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Email:     email,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, ErrListingFailed
	}

	return &ListUsersResult{
		Users:     users,
		PageToken: iter.PageState(),
	}, nil
}
