"""Unit tests for authentication."""
import pytest
from app.auth.service import AuthService


class TestAuthService:
    """Test authentication service."""
    
    def test_password_hashing(self):
        """Test password hashing."""
        password = "testpassword123"
        hashed = AuthService.hash_password(password)
        
        assert hashed != password
        assert AuthService.verify_password(password, hashed)
        assert not AuthService.verify_password("wrongpassword", hashed)
    
    def test_token_creation(self):
        """Test token creation."""
        token = AuthService.create_access_token(
            user_id="user-1",
            tenant_id="tenant-1",
            email="user@example.com",
        )
        
        assert isinstance(token, str)
        assert len(token) > 0
    
    def test_token_verification(self):
        """Test token verification."""
        token = AuthService.create_access_token(
            user_id="user-1",
            tenant_id="tenant-1",
            email="user@example.com",
        )
        
        success, token_data, error = AuthService.verify_token(token)
        
        assert success
        assert token_data.user_id == "user-1"
        assert token_data.tenant_id == "tenant-1"
        assert token_data.email == "user@example.com"
    
    def test_invalid_token(self):
        """Test invalid token handling."""
        success, token_data, error = AuthService.verify_token("invalid-token")
        
        assert not success
        assert token_data is None
        assert error is not None
