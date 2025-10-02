package protocol

import (
	"fmt"
)

// UserRecord represents user record: user_id, gender, birthdate, registered_at
type UserRecord struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

const UserRecordParts = 4

// Serialize returns the string representation of the user record
func (u *UserRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s", u.UserID, u.Gender, u.Birthdate, u.RegisteredAt)
}

// GetType returns the dataset type for user records
func (u *UserRecord) GetType() DatasetType {
	return DatasetTypeUsers
}

// NewUserRecordFromParts creates a UserRecord from string parts
func NewUserRecordFromParts(parts []string) (*UserRecord, error) {
	if len(parts) < UserRecordParts {
		return nil, fmt.Errorf("invalid UserRecord format: expected %d fields, got %d",
			UserRecordParts, len(parts))
	}

	return &UserRecord{
		UserID:       parts[0],
		Gender:       parts[1],
		Birthdate:    parts[2],
		RegisteredAt: parts[3],
	}, nil
}
